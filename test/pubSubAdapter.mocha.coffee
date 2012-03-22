racer = require 'racer/lib/racer'
shouldBehaveLikePubSubAdapter = require 'racer/test/pubSubAdapter'

plugin = require '../src'

options = pubSub: type: 'Redis'

describe 'Redis PubSub adapter', ->
  shouldBehaveLikePubSubAdapter options, [plugin]
