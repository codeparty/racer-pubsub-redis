redis = require 'redis'
patternInterface = require './channel-interface-pattern'
prefixInterface = require './channel-interface-prefix'
stringInterface = require './channel-interface-string'

exports = module.exports = (racer, opts = {}) ->
  racer.mixin
    type: 'Store'
    events:
      init: (store) ->
        {port, host, db, password} = opts
        subClient = redis.createClient port, host, opts
        pubClient = redis.createClient port, host, opts

        if password
          throwOnErr = (err) => throw err if err
          subClient.password password, throwOnErr
          pubClient.password password, throwOnErr

        ns = (db || 0) + '.'
        nsLen = ns.length
        prefix = (x) -> ns + x
        unprefix = (x) -> x.slice nsLen

        pubSub = store._pubSub

        pubSub.on 'disconnect', ->
          pubClient.end()
          subClient.end()

        if opts.pattern
          pubSub.defChannelInterface 'pattern', patternInterface pubSub, subClient, pubClient, prefix, unprefix
        if opts.prefix
          pubSub.defChannelInterface 'prefix', prefixInterface pubSub, subClient, pubClient, prefix, unprefix
        if opts.string
          pubSub.defChannelInterface 'string', stringInterface pubSub, subClient, pubClient, prefix, unprefix

        if opts.debug
          ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe'].forEach (event) ->
            subClient.on event, (channel, count) ->
              console.log "#{event.toUpperCase()} #{channel} COUNT = #{count}"
          subClient.on 'message', (channel, message) ->
            console.log "MESSAGE #{channel} #{message}"
          subClient.on 'pmessage', (pattern, channel, message) ->
            console.log "PMESSAGE #{pattern} #{channel} #{message}"
          __publish = subClient.publish
          subClient.publish = (channel, msg) ->
            console.log "PUBLISH #{channel} #{JSON.stringify message}"
            __publish.call subClient, channel, message


  return

exports.useWith = server: true, browser: false
