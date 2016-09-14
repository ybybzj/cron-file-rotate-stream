var cron = require('node-cron');
var glob = require('glob');
var fs = require('fs-extra');
var Writable = require('readable-stream').Writable;
var debug = require('debug')('cron-file-rotate-stream');
var xtend = require('xtend');
var rm = require('rimraf');
var $util = require('util');

var $path = require('path');
var noop = function(){};
var defaultOptions = {
  cron: '0 0 0 * * *',
  format: 'Y-M-D',//Y: year, M: month, D: month day, h: hour, m: munite, s:second  
  fileNum: 14
};

var assert = require('assert');

function CronFileRotateStream(filepath, opts){
  if(! this instanceof CronFileRotateStream){
    return new CronFileRotateStream(filepath, opts);
  }
  assert(typeof filepath === 'string' && filepath.trim(), '[cron-file-rotate-stream]invalid filepath! given: ' + filepath);
  opts = xtend({}, defaultOptions, opts);

  Writable.call(this, Object(opts.streamOptions) === opts.streamOptions ? opts.streamOptions : {} );

  this.cronPattern = opts.cron;
  this.format = opts.format;
  this.fileNum = opts.fileNum;
  this.encoding = opts.encoding || 'utf8';
  this.fileObj = xtend($path.parse(filepath), {path: filepath});
  this.cache = [];
  this.stream = this.getStream();
  this.task = cron.schedule(opts.cron, this.onCron.bind(this), false);
  this.cleanup = this.cleanup.bind(this);
  this.on('finish', this.cleanup);

  this.removeOldFiles((function(err){
    if(err){
      this.emit('error', err);
    }
  }).bind(this)); 
}

$util.inherits(CronFileRotateStream, Writable);
var proto = CronFileRotateStream.prototype;

proto.startCronTask = function(){
  if(this.task && !this.taskRunning){
    this.task.start();
    this.taskRunning = true;
  }
};
proto.stopCronTask = function(){

};
proto.getStream = function(){
  var fpath = this.fileObj.path; 
  var self = this;
  fs.ensureFileSync(fpath);

  var stream = fs.createWriteStream(fpath, {
    flags: 'a',
    defaultEncoding: this.encoding,
    fd: null,
    mode: 0o666
  });

  stream.on('error', function(err){
    self.emit('error', err);
  });
  return stream;
};

proto.rotate = function(cb){
  var backupFilepath = $path.format({
    root: this.fileObj.root,
    dir: this.fileObj.dir,
    base: this.fileObj.name + '_' + getDateStr(this.format) + this.fileObj.ext
  }),
  oldPath = this.fileObj.path,
    self = this;
    if(this.stream){
      this.stream.once('finish', function(){
        self.stream = null;
        debug('archiving log to %s...', backupFilepath);
        fs.rename(oldPath, backupFilepath, self.removeOldFiles.bind(self, cb));
      });

      this.stream.end();
    }else{
      debug('archiving log to %s...', backupFilepath);
      fs.rename(oldPath, backupFilepath, this.removeOldFiles.bind(this, cb));
    }

};

proto.removeOldFiles = function(cb, err){
  var self = this, cb = cb || noop;
  if(err){
    cb(err);
    return;
  }
  glob(this.fileObj.name + '_*' + this.fileObj.ext, function(er, files){
    if(er){
      cb(err);
      return;
    }
    files = files.sort(function(file1, file2){
      var ts1 = getTimeStamp(file1);
      var ts2 = getTimeStamp(file2);
      return ts1 - ts2;
    });

    var oldFileLen = files.length - self.fileNum;
    if(oldFileLen > 0){
      debug('remove old log files...');
      debug('%j', files.slice(0, oldFileLen));
      rmFiles(files.slice(0, oldFileLen), function(err){
        if(err){
          cb(err);
          return;
        }
        cb();
      });
    }else{
      cb();
    }

  });
};

proto.onCron = function(){
  debug('rotate start ...');
  this.isCaching = true;
  var self = this;
  this.rotate(function(err){
    if(err){
      self.emit('error', err);
    }
    self.stream = self.getStream();
    if(self.cache.length > 0){
      debug('flush cache start...');
      self.flushCache(function(){
        debug('flush cache finished!');
        self.isCaching = false;
      });
    }else{
      self.isCaching = false;
    }
  });
};

proto._write = function(chunk, enc, next){
  this.startCronTask();
  if(this.isCaching){
    debug('[cache]write to cache...........');
    this.writeToCache(chunk, enc, next);
  }else{
    this.writeToFile(chunk, enc, next);
  }
};

proto.writeToFile = function(chunk, enc, next){
  if(this.stream && !this.isCaching){ 
    this.stream.write(chunk, this.encoding);
    next();
  }else{
    debug('[file]write to cache...........');
    this.writeToCache(chunk, enc, next);
  }
};

proto.writeToCache = function(chunk, enc, next){
  this.cache.push(chunk);
  next();
};
proto.flushCache = function(cb){
  var cache = this.cache,
    i = 0, l = cache.length,
      stream = this.stream,
        encoding = this.encoding,
          self = this;

          function write(){
            var suc = true;
            while(i < l && suc) {
              var chunk = cache[i];
              if(i === l - 1){
                self.cache.length = 0;
                stream.write(chunk, encoding, cb);
              }else{
                suc = stream.write(chunk, encoding);
              }
              i += 1;
            }

            if(i < l){
              i -= 1;
              stream.once('drain', write);
            }
          }

          write();
};

proto.cleanup = function(){
  var self = this;

  debug('cleanup start ...');
  if(this.task){
    this.task.stop();
    this.task.destroy();
  }
  if(this.stream){
    this.flushCache(function(){
      self.stream.end();
    });
  }


};


//helper
function isFileExist(fp){
  try{
    var stat = fs.statSync(fp);
    return stat.isFile();
  }catch(e){
    return false;
  }
}

function getDateStr(fmt){
  var curDate = new Date();
  var year = curDate.getFullYear();
  var month = curDate.getMonth() + 1;
  var day = curDate.getDate();
  var minutes = curDate.getMinutes();
  var sec = curDate.getSeconds();
  var hours = curDate.getHours();
  return fmt.replace(/(Y|M|D|m|h|s)/g, function(m, t){
    switch(t){
      case 'Y': return year;
      case 'M': return month;
      case 'D': return day;
      case 'h': return hours;
      case 'm': return minutes;
      case 's': return sec;
    };
  });

}

function getTimeStamp(filepath){
  var stat;
  try{
    stat = fs.statSync(filepath);
    return stat.ctime.getTime();
  }catch(e){
    return -1;
  }

}

function rmFiles(files, cb){
  var i = 0, l = files.length, errState = null, count = 0;
  for(; i < l; i++){
    rm(files[i], function(err){
      if(err){
        errState = errState || err;
      }
      count += 1;
      if(count === l){
        cb(errState);
      }
    });
  }
}

module.exports = CronFileRotateStream;

//test
var rotateStream = new CronFileRotateStream($path.join(__dirname, 'test.log'), {
  cron: '*/5 * * * * *',
  format: 'h-m-s',
  fileNum: 20
});

var i = 0;
var timer = setInterval(function(){
  rotateStream.write(''+i+'\n', 'utf8');
  if(i > 1500){
    rotateStream.end();
    clearInterval(timer);
  }
  i += 1;
}, 10);

