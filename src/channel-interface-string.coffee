module.exports = stringInterface = (pubSub, subClient, pubClient, prefix, unprefix) ->
  # ns + string -> (subscriberId -> true)
  forwardIndex = {}

  # subscriberId -> (ns + string -> true)
  reverseIndex = {}

  subClient.on 'message', (nsStr, payload) ->
    if subscribers = forwardIndex[nsStr]
      {type, data} = JSON.parse payload
      for subscriberId of subscribers
        pubSub.emit type, subscriberId, data
    return

  # ns + str -> [{cb: ackCallback, timeout: ackErrorTimeout}, ...]
  pendingSubAcks = {}
  pendingUnsubAcks = {}

  subClient.on 'subscribe', (nsStr, numSubscribers) ->
    return unless acks = pendingSubAcks[nsStr]
    if ack = acks.shift()

      # Garbage collect empty ack arrays
      unless acks.length
        delete pendingSubAcks[nsStr]

      clearTimeout ack.timeout
      ack.cb null

  subClient.on 'unsubscribe', (nsStr, numSubscribers) ->
    return unless acks = pendingUnsubAcks[nsStr]
    if ack = acks.shift()

      # Garbage collect empty ack arrays
      unless acks.length
        delete pendingUnsubAcks[nsStr]

      clearTimeout ack.timeout
      ack.cb null

  return {
    subscribe: (subscriberId, str, ackCb) ->
      nsStr = prefix str

      (reverseIndex[subscriberId] ||= {})[nsStr] = true

      unless subsForStr = forwardIndex[nsStr]
        subsForStr = forwardIndex[nsStr] = {}
        subsForStr[subscriberId] = true

        if ackCb
          acks = pendingSubAcks[nsStr] ||= []
          timeout = setTimeout ->
            ackCb new Error "No ack for subscribe(#{subscriberId}, #{str})"
          , 200
          acks.push
            cb: ackCb
            timeout: timeout

        subClient.subscribe nsStr

      else
        subsForStr[subscriberId] = true
        ackCb? null

    unsubscribe: (subscriberId, str, ackCb) ->
      if typeof str isnt 'string'
        # Unsubscribe this subscriberId from all patterns
        ackCb = str

        # Send an ack immediately if this subscriberId is not subscribed to any
        # strings
        return ackCb? null unless stringsForSubscriber = reverseIndex[subscriberId]

        stringsRedisUnsub = []
        for nsStr of stringsForSubscriber
          subscribersForStr = forwardIndex[nsStr]
          # Clean up forwardIndex
          delete subscribersForStr[subscriberId]
          unless hasKeys subscribersForStr
            delete forwardIndex[nsStr]
            # Mark this nsStr to redis.unsubscribe since no more local
            # subscriberIds are subscribed to nsStr
            stringsRedisUnsub.push nsStr

        # Clean up reverseIndex
        delete reverseIndex[subscriberId]

        if ackCb
          # Ack right away if we don't need to send unsubscribe to redis.
          # This occurs in cases where we unsubscribe subscriberId from a
          # string, but more subscriberIds are still subscribed to that string.
          return ackCb null unless redisUnsubCount = stringsRedisUnsub.length

          # Set up acks for every pattern we need to redis.unsubscribe

          if redisUnsubCount > 1
            ackCb = finishAfter redisUnsubCount, ackCb
          for nsStr in stringsRedisUnsub
            acks = pendingUnsubAcks[nsStr] ||= []
            timeout = setTimeout ->
              ackCb new Error "No ack for unsubscribe(#{subscriberId})"
            , ACK_TIMEOUT_MS
            acks.push
              cb: ackCb
              timeout: timeout
        # endif ackCb

        # Send unsubscribes to redis
        for nsStr in stringsRedisUnsub
          subClient.unsubscribe nsStr
      # endif typeof pattern isnt 'string'
      else
        nsStr = prefix str

        # Clean up reverseIndex
        if subs = reverseIndex[subscriberId]
          delete subs[nsStr]
          unless hasKeys subs
            delete reverseIndex[subscriberId]

        # Clean up forwardIndex
        if subs = forwardIndex[nsStr]
          delete subs[subscriberId]
          unless hasKeys subs
            delete forwardIndex[nsStr]

        unless nsStr of forwardIndex
          if ackCb
            acks = pendingUnsubAcks[nsStr] ||= []
            timeout = setTimeout ->
              ackCb new Error "No ack for unsubscribe(#{subscriberId}, #{str})"
            , ACK_TIMEOUT_MS
            acks.push
              cb: ackCb
              timeout: timeout
          subClient.unsubscribe nsStr

      return

    publish: ({type, params}) ->
      switch type
        when 'direct', 'txn'
          pubClient.publish prefix(params.channel), JSON.stringify {type, data: params.data}

    hasSubscriptions: (subscriberId) -> subscriberId of reverseIndex

    subscribedTo: (subscriberId, str) ->
      return false unless stringsForSubscriber = reverseIndex[subscriberId]
      return path of stringsForSubscriber
  }
