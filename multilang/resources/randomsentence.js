/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Example for storm spout. Emits random sentences.
 * The original class in java - storm.starter.spout.RandomSentenceSpout.
 *
 */

var storm = require('./storm');
var Spout = storm.Spout;

var tweet_t;

var simplequeue = require('./node_modules/simplequeue');
var queue = simplequeue.createQueue("tweets");

var twitter = require('./node_modules/ntwitter');

var twit = new twitter({
        consumer_key: 'v9BIUekmGglxjrty77VbNZdPM',
        consumer_secret: 'qD4mnlXLHPdhbDOrZvGSRJjv0UEzwxZD3cVbrSqQbWgMXRDn83',
        access_token_key: '25076520-1T97cLiNDmjqAgFcBDcnwPURkyxh1Ta6WQlTSZkpS',
        access_token_secret: 'NWnJflgeXljmeKtzkbei3oigd1ung2lahm3vYxJje7M60'
    });

var trackwords = ['hate'];

function RandomSentenceSpout() {
    Spout.call(this);
    this.runningTupleId = 0;
    this.pending = {};
    var self = this;

        twit.stream('statuses/filter', {track:trackwords}, function(stream) {
        stream.on('data', function (data) {
            tweet_t = JSON.stringify(data);
            queue.putMessage(tweet_t);
        });
    });
};

// function temp(stream) {
//     stream.on('data', function (data) {
//         tweet_t = data;
//         queue.putMessage(data);
//     });
// }

RandomSentenceSpout.prototype = Object.create(Spout.prototype);
RandomSentenceSpout.prototype.constructor = RandomSentenceSpout;

RandomSentenceSpout.prototype.nextTuple = function(done) {
    var self = this;
    var id = this.createNextTupleId();

    setTimeout(function() {
        var message = queue.getMessageSync();
        queue.putMessage(message);

        if(message != null) {
            while(true) {
                var arr = [];
                // message = queue.getMessageSync();
                    arr.push(tweet_t);
                    self.emit({tuple: arr, id: id}, function(taskIds) {
                    // self.log('GOT THIS FROM THE QUEUE :  ' + message);
                    });
                    done();
            }
        }
    }, 100);

    done();
}

RandomSentenceSpout.prototype.createNextTupleId = function() {
    var id = this.runningTupleId;
    this.runningTupleId++;
    return id;
}

RandomSentenceSpout.prototype.ack = function(id, done) {
    this.log('Received ack for - ' + id);
    delete this.pending[id];
    done();
}

RandomSentenceSpout.prototype.fail = function(id, done) {
    var self = this;
    this.log('Received fail for - ' + id + '. Retrying.');
    this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
        self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
    });
    done();
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

new RandomSentenceSpout().run();