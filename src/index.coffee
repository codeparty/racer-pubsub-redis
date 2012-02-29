redis = require 'redis'
transaction = null
pathRegExp = null
hasKeys = null

module.exports = (racer) ->
  {transaction} = racer
  {regExp: pathRegExp} = racer.path
  {hasKeys} = racer.util
  racer.adapters.pubSub.Redis = PubSubRedis

PubSubRedis = (options = {}) ->
  self = this
  {port, host, db, password} = options
  namespace = (db || 0) + '.'
  @_prefixWithNamespace = (path) -> namespace + path

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
      console.log "PUBLISH #{@_prefixWithNamespace path} #{JSON.stringify message}"
      @__publish path, message

  subClient.on 'message', (path, message) ->
    if subs = pathSubs[path]
      message = JSON.parse message
      for subscriberId of subs
        self.onMessage subscriberId, message

  subClient.on 'pmessage', (pattern, path, message) ->
    if subs = patternSubs[pattern]
      message = JSON.parse message
      for subscriberId, re of subs
        self.onMessage subscriberId, message  if re.test path

  # Redis doesn't support callbacks on subscribe or unsubscribe methods, so
  # we call the callback after subscribe/unsubscribe events are published on
  # each of the paths for a given call of subscribe/unsubscribe.
  makeCallback = (queue, event) ->
    subClient.on event, (path, subscriberCount) ->
      if pending = queue[path]
        if obj = pending.shift()
          obj.out[path] = subscriberCount
          --obj.count || obj.callback null, obj.out
      return

  makeCallback @_pendingPsubscribe = {}, 'psubscribe'
  makeCallback @_pendingPunsubscribe = {}, 'punsubscribe'
  makeCallback @_pendingSubscribe = {}, 'subscribe'
  makeCallback @_pendingUnsubscribe = {}, 'unsubscribe'

  return

PubSubRedis:: =

  onMessage: ->

  disconnect: ->
    @_pubClient.end()
    @_subClient.end()

  publish: (path, message) ->
    path = @_prefixWithNamespace path
    @_pubClient.publish path, JSON.stringify message

  subscribe: (subscriberId, paths, callback, isLiteral) ->
    return if subscriberId is undefined

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
      path = @_prefixWithNamespace path
      if isLiteral
        value = true
      else
        value = pathRegExp path
        path += '*'

      toAdd.push path
      s = subs[path] ||= {}
      s[subscriberId] = ss[path] = value

    send toAdd, callbackQueue, @_subClient, method, callback

  unsubscribe: (subscriberId, paths, callback, isLiteral) ->
    return if subscriberId is undefined

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
    if paths
      toRemove = []
      delete ss[path]  if ss
      for path in paths
        path = @_prefixWithNamespace path
        path += '*' unless isLiteral
        if s = subs[path]
          delete s[subscriberId]
          toRemove.push path  unless hasKeys pathSubs

    else
      toRemove = (ss && Object.keys ss) || []

    send toRemove, callbackQueue, @_subClient, method, callback

  hasSubscriptions: (subscriberId) ->
    (subscriberId of @_subscriberPatternSubs) || (subscriberId of @_subscriberPathSubs)

  subscribedToTxn: (subscriberId, txn) ->
    path = @_prefixWithNamespace transaction.path txn
    for p, re of @_subscriberPatternSubs[subscriberId]
      return true if re.test path
    return false

send = (paths, queue, client, method, callback) ->
  return callback?()  unless i = paths.length

  obj = {callback, out: {}, count: 1}  if callback
  while i--
    path = paths[i]
    (queue[path] ||= []).push obj  if callback
    client[method] path
