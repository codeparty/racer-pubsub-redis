racer = require 'racer/lib/racer'
shouldBehaveLikePubSubAdapter = require 'racer/test/pubSubAdapter'

storeOpts = {}
plugin = require '../src'
plugin.testOpts = pattern: true, prefix: true, string: true


describe 'Redis PubSub adapter', ->
  shouldBehaveLikePubSubAdapter storeOpts, [plugin]
