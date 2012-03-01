{EventEmitter} = require 'events'
redis = require 'redis'
pathRegExp = hasKeys = null

module.exports = (racer) ->
  {regExp: pathRegExp} = racer.path
  {hasKeys} = racer.util
  racer.adapters.pubSub.Redis = PubSubRedis

PubSubRedis = (options = {}) ->
  self = this
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

  @_pathSubs = pathSubs = {}
  @_patternSubs = patternSubs = {}
  @_subscriberPathSubs = {}
  @_subscriberPatternSubs = {}

  if options.debug
    for event in ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe']
      do (event) ->
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

  subClient.on 'message', (path, message) ->
    if subs = pathSubs[path]
      message = JSON.parse message
      for subscriberId of subs
        self.emit 'message', subscriberId, message

  subClient.on 'pmessage', (pattern, path, message) ->
    if (subs = patternSubs[pattern]) && subsMatchPath subs, path
      message = JSON.parse message
      for subscriberId of subs
        self.emit 'message', subscriberId, message

  # Redis doesn't support callbacks on subscribe or unsubscribe methods, so
  # we call the callback after subscribe/unsubscribe events are published on
  # each of the paths for a given call of subscribe/unsubscribe.
  makeCallback = (queue, event) ->
    subClient.on event, (path, subscriberCount) ->
      if pending = queue[path]
        if obj = pending.shift()
          --obj.count || obj.callback()
      return

  makeCallback @_pendingPsubscribe = {}, 'psubscribe'
  makeCallback @_pendingPunsubscribe = {}, 'punsubscribe'
  makeCallback @_pendingSubscribe = {}, 'subscribe'
  makeCallback @_pendingUnsubscribe = {}, 'unsubscribe'

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
      (if isLiteral then @_prefix(path) + '*' else @_prefix(path)  for path in paths)
    else
      (ss && Object.keys ss) || []

    toRemove = []
    for prefixed in paths
      delete ss[prefixed]  if ss
      if s = subs[prefixed]
        delete s[subscriberId]
        unless hasKeys s
          delete subs[prefixed]
          toRemove.push prefixed
          @emit 'noSubscribers',
            if isLiteral then @_unprefix(prefixed) else @_unprefix(prefixed)[0..-2]

    send toRemove, callbackQueue, @_subClient, method, callback

  hasSubscriptions: (subscriberId) ->
    (subscriberId of @_subscriberPatternSubs) || (subscriberId of @_subscriberPathSubs)

  subscribedTo: (subscriberId, path) ->
    path = @_prefix path
    for p, re of @_subscriberPatternSubs[subscriberId]
      return true if re.test path
    return false


send = (paths, queue, client, method, callback) ->
  return callback?()  unless i = paths.length

  obj = {callback, count: i}  if callback
  while i--
    path = paths[i]
    (queue[path] ||= []).push obj  if callback
    client[method] path

subsMatchPath = (subs, path) ->
  for subscriberId, re of subs
    return re.test path
