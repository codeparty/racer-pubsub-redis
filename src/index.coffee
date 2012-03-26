{EventEmitter} = require 'events'
redis = require 'redis'
pathRegExp = hasKeys = null
pathRegExp = require('racer/lib/path').regExp
{hasKeys} = require 'racer/lib/util'

exports = module.exports = (racer) ->
  racer.registerAdapter 'pubSub', 'Redis', PubSubRedis

exports.useWith = server: true, browser: false

PubSubRedis = (options = {}) ->
  EventEmitter.call this
  {port, host, db, password} = options
  namespace = (db || 0) + '.'
  @_prefix = (path) -> namespace + path
  @_unprefix = (path) -> path.slice namespace.length

  @_pubClient = pubClient = redis.createClient port, host, options
  @_subClient = subClient = redis.createClient port, host, options

  if password
    throwOnErr = (err) -> throw err if err
    pubClient.auth password, throwOnErr
    subClient.auth password, throwOnErr

  # path -> (namespace + path -> (subscriberId -> true))
  @_pathSubs = pathSubs = {}

  # pattern -> (ns + path + '*' -> (subscriberId -> RegExp))
  @_patternSubs = patternSubs = {}

  # subscriberId -> (namespace + path -> true)
  @_subscriberPathSubs = {}

  # subscriberId -> (namespace + path + '*' -> RegExp)
  @_subscriberPatternSubs = {}

  if options.debug
    ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe'].forEach (event) ->
      subClient.on event, (path, count) ->
        console.log "#{event.toUpperCase()} #{path} COUNT = #{count}"
    subClient.on 'message', (channel, message) ->
      console.log "MESSAGE #{channel} #{message}"
    subClient.on 'pmessage', (pattern, channel, message) ->
      console.log "PMESSAGE #{pattern} #{channel} #{message}"
    @__publish = PubSubRedis::publish
    @publish = (path, message) ->
      console.log "PUBLISH #{@_prefix path} #{JSON.stringify message}"
      @__publish path, message

  subClient.on 'message', (path, message) =>
    if subs = pathSubs[path]
      message = JSON.parse message
      for subscriberId of subs
        @emit 'message', subscriberId, message
    return

  subClient.on 'pmessage', (pattern, path, message) =>
    if (subs = patternSubs[pattern]) && subsMatchPath subs, path
      message = JSON.parse message
      for subscriberId of subs
        @emit 'message', subscriberId, message
    return

  # Redis doesn't support callbacks on subscribe or unsubscribe methods, so
  # we call the callback after subscribe/unsubscribe events are published on
  # each of the paths for a given call of subscribe/unsubscribe.
  makeCallback = (queue) ->
    return (path, subscriberCount) ->
      if pending = queue[path]
        if obj = pending.shift()
          --obj.count || obj.callback()
      return

  subClient.on 'psubscribe', makeCallback(@_pendingPsubscribe = {})
  subClient.on 'punsubscribe', makeCallback(@_pendingPunsubscribe = {})
  subClient.on 'subscribe', makeCallback(@_pendingSubscribe = {})
  subClient.on 'unsubscribe', makeCallback(@_pendingUnsubscribe = {})

  return

PubSubRedis:: =
  __proto__: EventEmitter::

  disconnect: ->
    @_pubClient.end()
    @_subClient.end()

  publish: (path, message) ->
    @_pubClient.publish @_prefix(path), JSON.stringify(message)

  subscribe: (subscriberId, paths, callback, isLiteral) ->
    if isLiteral
      method = 'subscribe'
      callbackQueue = @_pendingSubscribe
      subs = @_pathSubs
      subscriberSubs = @_subscriberPathSubs
    else
      method = 'psubscribe'
      callbackQueue = @_pendingPsubscribe
      subs = @_patternSubs
      subscriberSubs = @_subscriberPatternSubs

    toAdd = []
    ss = subscriberSubs[subscriberId] ||= {}
    for path in paths
      prefixed = @_prefix path
      if isLiteral
        value = true
      else
        value = pathRegExp prefixed
        prefixed += '*'

      toAdd.push prefixed
      s = subs[prefixed] ||= {}
      s[subscriberId] = ss[prefixed] = value

    send toAdd, callbackQueue, @_subClient, method, callback

  unsubscribe: (subscriberId, paths, callback, isLiteral) ->
    if isLiteral
      method = 'unsubscribe'
      callbackQueue = @_pendingUnsubscribe
      subs = @_pathSubs
      subscriberSubs = @_subscriberPathSubs
    else
      method = 'punsubscribe'
      callbackQueue = @_pendingPunsubscribe
      subs = @_patternSubs
      subscriberSubs = @_subscriberPatternSubs

    ss = subscriberSubs[subscriberId]
    paths = if paths
      for path in paths
        if isLiteral then @_prefix(path) else @_prefix(path) + '*'
    else
      (ss && Object.keys ss) || []

    toRemove = []
    for prefixed in paths
      delete ss[prefixed]  if ss
      continue unless s = subs[prefixed]

      delete s[subscriberId]
      continue if hasKeys s

      delete subs[prefixed]
      toRemove.push prefixed
      @emit 'noSubscribers',
        if isLiteral then @_unprefix(prefixed) else @_unprefix(prefixed)[0..-2]

    send toRemove, callbackQueue, @_subClient, method, callback

  hasSubscriptions: (subscriberId) ->
    (subscriberId of @_subscriberPatternSubs) || (subscriberId of @_subscriberPathSubs)

  subscribedTo: (subscriberId, path) ->
    path = @_prefix path
    subs = @_subscriberPatternSubs[subscriberId]
    return subsMatchPath subs, path


send = (paths, queue, client, method, callback) ->
  return callback?()  unless i = paths.length

  obj = {callback, count: i}  if callback
  while i--
    path = paths[i]
    (queue[path] ||= []).push obj  if callback
    client[method] path

subsMatchPath = (subs, path) ->
  for subscriberId, re of subs
    return true if re.test path
  return false
