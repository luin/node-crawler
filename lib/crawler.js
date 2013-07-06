var http = require('http'),
path = require('path'),
url = require('url'),
request = require('request'),
_ = require('underscore'),
jschardet = require('jschardet'),
zlib = require("zlib"),
fs = require("fs"),
Pool = require('generic-pool').Pool;

// Fallback on iconv-lite if we didn't succeed compiling iconv
// https://github.com/sylvinus/node-crawler/pull/29
var iconv, iconvLite;
try {
  iconv = require('iconv').Iconv;
} catch (e) {}

if (!iconv) {
  iconvLite = require('iconv-lite');
}


exports.VERSION = "0.2.4";

exports.Crawler = function(options) {

  var self = this;

  //Default options
  self.options = _.extend({
    timeout:        60000,
    parser:         'cheerio',
    maxConnections: 10,
    priorityRange:  10,
    priority:       5,
    retries:        3,
    forceUTF8:      false,
    userAgent:      "node-crawler/"+exports.VERSION,
    autoWindowClose:true,
    retryTimeout:   10000,
    method:         "GET",
    cache:          false, //false, or a cache object
    onDrain:        false
  },options);

  // Don't make these options persist to individual queries
  var masterOnlyOptions = ["maxConnections", "priorityRange", "onDrain"];
  //Setup a worker pool w/ https://github.com/coopernurse/node-pool
  self.pool = Pool({
    name         : 'crawler',
    //log        : self.options.debug,
    max          : self.options.maxConnections,
    priorityRange: self.options.priorityRange,
    create       : function(callback) {
      callback(1);
    },
    destroy      : function(client) {

    }
  });

  var plannedQueueCallsCount = 0;
  var queuedCount = 0;

  var release = function(opts) {

    queuedCount--;
    // console.log("Released... count",queuedCount,plannedQueueCallsCount);

    if (opts._poolRef) self.pool.release(opts._poolRef);

    // Pool stats are behaving weird - have to implement our own counter
    // console.log("POOL STATS",{"name":self.pool.getName(),"size":self.pool.getPoolSize(),"avail":self.pool.availableObjectsCount(),"waiting":self.pool.waitingClientsCount()});

    if (queuedCount+plannedQueueCallsCount === 0) {
      if (self.options.onDrain) self.options.onDrain();
    }
  };

  self.onDrain = function() {};

  var useCache = function(opts) {
    return (opts.uri && opts.cache && (opts.method === "GET" || opts.method === "HEAD"));
  };

  self.request = function(opts) {
    var run = function() {
      if (opts.debug) {
        console.log(opts.method+" "+opts.uri+" ...");
      }

      // Cloning keeps the opts parameter clean:
      // - some versions of "request" apply the second parameter as a
      // property called "callback" to the first parameter
      // - keeps the query object fresh in case of a retry
      // Doing parse/stringify instead of _.clone will do a deep clone and remove functions

      var ropts = JSON.parse(JSON.stringify(opts));

      if (!ropts.headers) {
        ropts.headers = {};
    }
      if (ropts.forceUTF8) {
        if (!ropts.headers["Accept-Charset"] && !ropts.headers["accept-charset"]) {
          ropts.headers["Accept-Charset"] = 'utf-8;q=0.7,*;q=0.3';
        }
        if (!ropts.encoding) {
          ropts.encoding = null;
        }
      }
      if (!ropts.encoding) {
        ropts.headers["Accept-Encoding"] = "gzip";
        ropts.encoding = null;
      }
      if (ropts.userAgent) {
        ropts.headers["User-Agent"] = ropts.userAgent;
      }

      var requestArgs = ["uri","url","qs","method","headers","body","form","json","multipart","followRedirect","followAllRedirects",
        "maxRedirects","encoding","pool","timeout","proxy","oauth","strictSSL","jar","aws"];


      var req = request(_.pick.apply(this,[ropts].concat(requestArgs)), function(error,response,body) {
        if (error) {
          return self.onContent(error, opts);
        }

        response.uri = opts.uri;

        // Won't be needed after https://github.com/mikeal/request/pull/303 is merged
        if (response.headers['content-encoding'] && response.headers['content-encoding'].toLowerCase().indexOf('gzip') >= 0) {
          zlib.gunzip(response.body, function (error, body) {
            if (error) {
              return self.onContent(error, opts);
            }

            response.body = body.toString(req.encoding);

            self.onContent(error,opts,response,false);
          });
        } else {
          self.onContent(error,opts,response,false);
        }

      });
    };
    if (useCache(opts)) {
      opts.cache.get(opts.uri, function(err, cacheData) {
        if (cacheData) {
          self.onContent(null, opts, JSON.parse(cacheData), true);
        } else {
          run();
        }
      });
    } else {
      run();
    }

  };

  self.onContent = function (error, toQueue, response, fromCache) {
    Object.keys(response).forEach(function(key) {
      if (key !== 'body') {
        delete response[key];
      }
    });
    if (error) {

      if (toQueue.debug) {
        console.log("Error "+error+" when fetching "+toQueue.uri+(toQueue.retries?" ("+toQueue.retries+" retries left)":""));
      }

      if (toQueue.retries) {
        plannedQueueCallsCount++;
        setTimeout(function() {
          toQueue.retries--;
          plannedQueueCallsCount--;
          self.queue(toQueue);
        },toQueue.retryTimeout);

      } else if (toQueue.callback) {
        toQueue.callback(error);
      }

      return release(toQueue);
    }

    if (!response.body) response.body="";

    if (toQueue.debug) {
      console.log("Got "+(toQueue.uri||"html")+" ("+response.body.length+" bytes)...");
    }

    if (toQueue.forceUTF8) {
      //TODO check http header or meta equiv?
      var detected = jschardet.detect(response.body);

      if (detected && detected.encoding) {
        if (toQueue.debug) {
          console.log("Detected charset "+detected.encoding+" ("+Math.floor(detected.confidence*100)+"% confidence)");
        }
        if (detected.encoding!="utf-8" && detected.encoding!="ascii") {

          if (iconv) {
            var iconvObj = new iconv(detected.encoding, "UTF-8//TRANSLIT//IGNORE");
            response.body = iconvObj.convert(response.body).toString();

            // iconv-lite doesn't support Big5 (yet)
          } else if (detected.encoding != "Big5") {
            response.body = iconvLite.decode(response.body, detected.encoding);
          }

        } else if (typeof response.body != "string") {
          response.body = response.body.toString();
        }

      } else {
        response.body = response.body.toString("utf8"); //hope for the best
      }

    } else {
      response.body = response.body.toString();
    }

    if (useCache(toQueue) && !fromCache) {
      if (toQueue.cache) {
        toQueue.cache.set(toQueue.uri, JSON.stringify(response, true));
      }
    }

    if (!toQueue.callback) {
      return release(toQueue);
    }

    response.options = toQueue;


    if (toQueue.parser && toQueue.parser === 'cheerio' && toQueue.method !== 'HEAD') {
      try {
        var $ = require('cheerio').load(response.body);
        toQueue.callback(null, response, $);
      } catch (err) {
        toQueue.callback(err);
        release(toQueue);
        throw err;
      }
      release(toQueue);
    } else {

      toQueue.callback(null,response);
      release(toQueue);
    }

  };

  self.queue = function(item) {

    //Did we get a list ? Queue all the URLs.
    if (Array.isArray(item)) {
      var i;
      for (i=0;i<item.length;i++) {
        self.queue(item[i]);
      }
      return;
    }

    queuedCount++;

    var toQueue=item;

    //Allow passing just strings as URLs
    if (_.isString(item)) {
      toQueue = {"uri":item};
    }

    _.defaults(toQueue,self.options);

    // Cleanup options
    _.each(masterOnlyOptions,function(o) {
      delete toQueue[o];
    });

    self.pool.acquire(function(err, poolRef) {

      //TODO - which errback to call?
      if (err) {
        console.error("pool acquire error:",err);
        return release(toQueue);
      }

      toQueue._poolRef = poolRef;

      //Static HTML was given, skip request
      if (toQueue.html) {
        self.onContent(null,toQueue,{body:toQueue.html},false);
        return;
      }

      //Make a HTTP request
      if (typeof toQueue.uri=="function") {
        toQueue.uri(function(uri) {
          toQueue.uri=uri;
          self.request(toQueue);
        });
      } else {
        self.request(toQueue);
      }

    },toQueue.priority);
  };

};



