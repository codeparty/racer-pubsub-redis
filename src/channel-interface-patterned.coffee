{EventEmitter} = require 'events'
finishAfter = hasKeys = pathRegExp = null

ACK_TIMEOUT_MS = 1000

module.exports = (racer) ->
  finishAfter = racer.util.async.finishAfter
  hasKeys = racer.util.hasKeys
  pathRegExp = racer.protected.path.regExp
  return {
    prefixInterface: patternedInterface 'prefix'
    patternInterface: patternedInterface 'pattern'
  }

patternedInterface = (ns) ->
  # TODO Rm unprefix if we never use it
  return (pubSub, subClient, pubClient, prefix, unprefix) ->

    # ns + channel + '*' -> {re: RegExp, subscribers: (subscriberId -> true)}
    forwardIndex = {}

    # subscriberId -> (ns + channel + '*' -> RegExp)
    reverseIndex = {}

    intf = {}

    subClient.on 'pmessage', (nsPatternStar, str, payload) ->
      return unless subs = forwardIndex[nsPatternStar]
      return unless subs.re.test unprefix(str).slice ns.length+1
      {type, data} = JSON.parse payload
      for subscriberId of subs.subscribers
        pubSub.emit type, subscriberId, data
      return

    # ns + pattern + '*' -> [{cb: ackCallback, timeout: ackErrorTimeout}, ...]
    pendingPsubAcks = {}
    pendingPunsubAcks = {}


    subClient.on 'psubscribe', (nsPatternStar, numSubscribers) ->
      return unless acks = pendingPsubAcks[nsPatternStar]
      if ack = acks.shift()

        # Garbage collect empty ack arrays
        unless acks.length
          delete pendingPsubAcks[nsPatternStar]

        clearTimeout ack.timeout
        ack.cb null

    subClient.on 'punsubscribe', (nsPatternStar, numSubscribers) ->
      return unless acks = pendingPunsubAcks[nsPatternStar]
      if ack = acks.shift()

        # Garbage collect empty ack arrays
        unless acks.length
          delete pendingPunsubAcks[nsPatternStar]

        clearTimeout ack.timeout
        ack.cb null

    intf.subscribe = (subscriberId, pattern, ackCb) ->
      re = pathRegExp pattern
      nsPattern = prefix(ns + '$' + pattern)
      nsPatternStar = nsPattern + '*'

      (reverseIndex[subscriberId] ||= {})[nsPatternStar] = re

      unless subsForPattern = forwardIndex[nsPatternStar]
        subsForPattern = forwardIndex[nsPatternStar] =
          subscribers: {}
          re: re
        subsForPattern.subscribers[subscriberId] = true

        if ackCb
          acks = pendingPsubAcks[nsPatternStar] ||= []
          timeout = setTimeout ->
            ackCb new Error "No ack for subscribe(#{subscriberId}, #{pattern})"
          , ACK_TIMEOUT_MS
          acks.push
            cb: ackCb
            timeout: timeout
        subClient.psubscribe nsPatternStar

      else
        subsForPattern.subscribers[subscriberId] = true
        ackCb? null

    intf.unsubscribe = (subscriberId, pattern, ackCb) ->
      if typeof pattern isnt 'string'
        # Unsubscribe this subscriberId from all patterns
        ackCb = pattern


        # Send an ack immediately if this subscriberId is not subscribed to any
        # patterns
        return ackCb? null unless patternsForSubscriber = reverseIndex[subscriberId]

        toPunsubscribe = []
        for nsPatternStar of patternsForSubscriber
          subscribersForPattern = forwardIndex[nsPatternStar]
          # Clean up forwardIndex
          delete subscribersForPattern[subscriberId]
          unless hasKeys subscribersForPattern
            delete forwardIndex[nsPatternStar]
            # Mark this pattern to redis.punsubscribe since no more local
            # subscriberIds are subscribed to the pattern
            toPunsubscribe.push nsPatternStar

        # Clean up reverseIndex
        delete reverseIndex[subscriberId]

        if ackCb
          # Ack right away if we don't need to send punsubscribe to redis.
          # This occurs in cases where we unsubscribe subscriberId from a
          # pattern, but more subscriberIds are still subscribed to that pattern.
          return ackCb null unless punsubCount = toPunsubscribe.length

          # Set up acks for every pattern we need to redis.punsubscribe

          if punsubCount > 1
            ackCb = finishAfter punsubCount, ackCb
          for nsPatternStar in toPunsubscribe
            acks = pendingPunsubAcks[nsPatternStar] ||= []
            timeout = setTimeout ->
              ackCb new Error "No ack for unsubscribe(#{subscriberId})"
            , ACK_TIMEOUT_MS
            acks.push
              cb: ackCb
              timeout: timeout
        # endif ackCb

        # Send punsubscribes to redis
        for nsPatternStar in toPunsubscribe
          subClient.punsubscribe nsPatternStar
      # endif typeof pattern isnt 'string'
      else
        nsPatternStar = prefix(ns + '$' + pattern) + '*'

        # Clean up reverseIndex
        if subs = reverseIndex[subscriberId]
          delete subs[nsPatternStar]
          unless hasKeys subs
            delete reverseIndex[subscriberId]

        # Clean up forwardIndex
        if subs = forwardIndex[nsPatternStar]
          {subscribers, re} = subs
          delete subscribers[subscriberId]
          unless hasKeys subscribers
            delete forwardIndex[nsPatternStar]

        unless nsPatternStar of forwardIndex
          if ackCb
            acks = pendingPunsubAcks[nsPatternStar] ||= []
            timeout = setTimeout ->
              ackCb new Error "No ack for unsubscribe(#{subscriberId}, #{pattern})"
            , ACK_TIMEOUT_MS
            acks.push
              cb: ackCb
              timeout: timeout
          subClient.punsubscribe nsPatternStar

      return

    intf.publish = ({type, params}) ->
      switch type
        when 'txn', 'ot'
          pubClient.publish prefix(ns + '$' + params.channel), JSON.stringify {type, data: params.data}

    intf.hasSubscriptions = (subscriberId) -> subscriberId of reverseIndex

    # TODO `path` is specific to our use of subscribedTo. Generalize it
    intf.subscribedTo = (subscriberId, path) ->
      for _, re of reverseIndex[subscriberId]
        return true if re.test path
      return false

    return intf
