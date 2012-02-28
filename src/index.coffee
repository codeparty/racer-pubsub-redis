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

  @_subs = subs = {}
  @_subscriberSubs = {}

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

  subClient.on 'pmessage', (pattern, path, message) ->
    # The pattern returned will have an extra * on the end
    if pathSubs = subs[pattern.substr(0, pattern.length - 1)]
      message = JSON.parse message
      for subscriberId, re of pathSubs
        self.onMessage subscriberId, message if re.test path

  subClient.on 'message', (path, message) ->
    if pathSubs = subs[path]
      message = JSON.parse message
      for subscriberId of pathSubs
        self.onMessage subscriberId, message

  # Redis doesn't support callbacks on subscribe or unsubscribe methods, so
  # we call the callback after subscribe/unsubscribe events are published on
  # each of the paths for a given call of subscribe/unsubscribe.
  makeCallback = (queue, event) ->
    subClient.on event, (path) ->
      if pending = queue[path]
        if callback = pending.shift()
          callback() unless --callback.__count
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

    subs = @_subs
    subscriberSubs = @_subscriberSubs
    toAdd = []
    for path in paths
      path = @_prefixWithNamespace path
      unless pathSubs = subs[path]
        subs[path] = pathSubs = {}
        toAdd.push path
      re = pathRegExp path
      pathSubs[subscriberId] = re
      ss = subscriberSubs[subscriberId] ||= {}
      ss[path] = re

    if isLiteral
      method = 'subscribe'
      callbackQueue = @_pendingSubscribe
    else
      method = 'psubscribe'
      callbackQueue = @_pendingPsubscribe
    handlePaths toAdd, callbackQueue, @_subClient, method, callback

  unsubscribe: (subscriberId, paths, callback, isLiteral) ->
    return if subscriberId is undefined

    # For signature: unsubscribe(subscriberId, callback)
    if typeof paths is 'function'
      callback = paths
      paths = null

    # For signature: unsubscribe(subscriberId[, callback])
    subscriberSubs = @_subscriberSubs
    paths ||= subscriberSubs[subscriberId] || []

    # For signature: unsubscribe(subscriberId, paths[, callback])
    subs = @_subs
    toRemove = []
    for path in paths
      path = @_prefixWithNamespace path
      if pathSubs = subs[path]
        delete pathSubs[subscriberId]
        toRemove.push path unless hasKeys pathSubs
      delete ss[path] if ss = subscriberSubs[subscriberId]

    if isLiteral
      method = 'subscribe'
      callbackQueue = @_pendingSubscribe
    else
      method = 'psubscribe'
      callbackQueue = @_pendingPsubscribe
    handlePaths toRemove, callbackQueue,  @_subClient, method, callback

  hasSubscriptions: (subscriberId) -> subscriberId of @_subscriberSubs

  subscribedToTxn: (subscriberId, txn) ->
    path = @_prefixWithNamespace transaction.path txn
    for p, re of @_subscriberSubs[subscriberId]
      return true if p == path || re.test path
    return false

handlePaths = (paths, queue, client, fn, callback) ->
  if i = paths.length
    callback.__count = i if callback
  else
    callback() if callback
  while i--
    path = paths[i]
    path += '*' if fn == 'psubscribe'
    client[fn] path
    if callback
      (queue[path] ||= []).push callback
