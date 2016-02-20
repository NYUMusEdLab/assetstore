'use strict';

const restify = require('restify');
const Datastore = require('nedb');
const path = require('path');
const fs = require('mz/fs');
const crypto = require('mz/crypto');
const mime = require('mime');
const bunyan = require('bunyan');

const port = process.env.PORT || 8080;
const dbLocation = process.env.DB_LOCATION || path.join(__dirname, 'temp.db');
const fileBaseDir = process.env.BASE_DIR || path.join(__dirname, 'files');
const db = new Datastore({ filename: dbLocation, autoload: true });
const log = bunyan.createLogger({
  name: 'assetstore',
  stream: process.stdout,
});

const server = restify.createServer();
server.use(restify.CORS());
server.pre(restify.pre.sanitizePath());
server.use(restify.bodyParser({
  mapParams: true,
  mapFiles: true,
}));
server.on('after', restify.auditLogger({
  log,
}));

// Path structure : store.musedlab.org/<session ID>/<keywordname>.wav
// Data model
// {
//   sessionId: 'xxx123',
//   assets: {
//     key: {
//       path: 'path/to/file',
//       modified: [Date object],
//       size: 12345, <In Bytes>,
//       mime: 'text/plain',
//       updated: <number of times updated>,
//       hash: <md5hash>,
//     },
//     metaphors: {
//       path: 'xxx123/metaphors.wav',
//       modified: 1455138621072,
//       updated: 3,
//       hash: f13debc5...
//     }
//   }
// }

server.get('/', (req, res, next) => {
  res.json(200, {
    status: 200,
    message: 'AssetStore API v1.0.0',
    api: 1,
    description: 'This API is a self-hosted version of AWS S3',
  });
  next();
});

/* ##################### GET /:session ########################### */
server.get('/:session', (req, res, next) => {
  const dirPath = path.join(fileBaseDir, req.params.session);

  log.info(`Incoming GET for session ${req.params.session}.`);
  db.find({
    sessionId: req.params.session,
  }, (err, docs) => {
    if (docs.length === 0) {
      log.info(`Session ID ${req.params.session} not found`);
      res.json(404, {
        status: 404,
        message: `Session ID ${req.params.session} not found`,
      });
    } else if (docs.length === 1) {
      const sessionObject = docs[0];
      res.json(200, sessionObject);
    }
  });
  next();
});

/* ##################### PUT & POST /:session/:filename ########################### */
function putAndPostHandler (req, res, next) {
  // const {session, filename, data} = req.params;
  const dirPath = path.join(fileBaseDir, req.params.session);
  const filePath = path.join(dirPath, req.params.filename);
  const fileKey = req.params.filename.split('.')[0];
  log.info(`Incoming PUT for session ${req.params.session} with file ${req.params.filename}, size: ${req.params.data.length} bytes`);
  db.find({
    sessionId: req.params.session,
  }, (err, docs) => {
    if (docs.length === 1) {
      const sessionObject = docs[0];
      log.info(sessionObject.assets[fileKey]);
      if (sessionObject.assets[fileKey]) {
        log.info('This asset already exists. It will be overwritten.');
      }
      fs.writeFile(filePath, req.params.data)
        .then(() => {
          return new Promise((resolve, reject) => {
            const md5hash = crypto.createHash('md5');
            md5hash.setEncoding('hex');
            const fd = fs.createReadStream(filePath);
            fd.on('end', () => {
              md5hash.end();
              resolve(md5hash.read());
            });
            fd.pipe(md5hash);
          });
        }, error => {
          return error;
        })
        .then((filehash) => {
          if (fs.statSync(filePath).size === req.params.data.length) {
            log.info('File on disk matches sent data. Assuming this means file was written successfully.');
            const assetModifiedObj = {};
            const assetUpdatedObj = {};
            assetModifiedObj[`assets.${fileKey}.modified`] = Date.now();
            assetModifiedObj[`assets.${fileKey}.size`] = req.params.data.length;
            assetModifiedObj[`assets.${fileKey}.mime`] = mime.lookup(filePath);
            assetModifiedObj[`assets.${fileKey}.path`] = filePath;
            assetModifiedObj[`assets.${fileKey}.hash`] = filehash;
            if (sessionObject.assets[fileKey]) {
              assetUpdatedObj[`assets.${fileKey}.updated`] = 1;
            } else {
              assetUpdatedObj[`assets.${fileKey}.updated`] = 0;
            }
            db.update({
              sessionId: req.params.session,
            }, {
              $set: assetModifiedObj,
              $inc: assetUpdatedObj,
            }, { upsert: true }, (error, numReplaced, upsert) => {
              log.info(`Document inserted: ${upsert}`);
              if (error) {
                return error;
              }
              log.info(`Number of docs replaced: ${numReplaced}`);
            });
          }
        }, error => {
          return error;
        })
        .then((data) => {
          res.json(200, {
            status: 200,
            message: 'File updated successfully',
            path: data,
          });
        }, (error) => {
          res.json(503, {
            status: 503,
            message: error,
          });
        });
    } else if (docs.length === 0) {
      fs.stat(dirPath)
        .then(stat => {
          return stat;
        }, error => {
          if (error.code === 'ENOENT') {
            return fs.mkdir(dirPath);
          } else {
            return error;
          }
        })
        .then(() => {
          return fs.writeFile(filePath, req.params.data);
        }, (error) => {
          return error;
        })
        .then(() => {
          return new Promise((resolve, reject) => {
            const md5hash = crypto.createHash('md5');
            md5hash.setEncoding('hex');
            const fd = fs.createReadStream(filePath);
            fd.on('end', () => {
              md5hash.end();
              resolve(md5hash.read());
            });
            fd.pipe(md5hash);
          });
        }, error => {
          return error;
        })
        .then((filehash) => {
          if (fs.statSync(filePath).isFile()) {
            const record = {};
            record.sessionId = req.params.session;
            record.assets = {};
            record.assets[fileKey] = {
              path: filePath,
              modified: Date.now(),
              size: req.params.data.length,
              mime: mime.lookup(filePath),
              updated: 0,
              hash: filehash,
            };
            db.insert(record, (error, doc) => {
              if (doc) {
                return filePath;
              } else {
                return error;
              }
            });
          }
        }, (error) => {
          return error;
        })
        .then((data) => {
          res.json(200, {
            status: 200,
            message: 'Files stored successfully',
            path: data,
          });
        }, (error) => {
          res.json(503, {
            status: 503,
            message: error,
          });
        });
    }
  });
  return next();
}

server.put('/:session/:filename', putAndPostHandler);
server.post('/:session/:filename', putAndPostHandler);

/* ##################### GET /:session//:filename ########################### */
server.get('/:session/:filename', (req, res, next) => {
  const dirPath = path.join(fileBaseDir, req.params.session);
  const filePath = path.join(dirPath, req.params.filename);
  const fileKey = req.params.filename.split('.')[0];
  db.find({
    sessionId: req.params.session,
  }, (err, docs) => {
    if (docs.length === 0) {
      log.info(`No record found with ID: ${req.params.session}`);
    } else if (docs.length === 1) {
      const sessionObject = docs[0];
      if (fs.statSync(sessionObject.assets[fileKey].path).isFile()) {
        res.writeHead(200, { 'Content-Type': sessionObject.assets[fileKey].mime });
        const fd = fs.createReadStream(sessionObject.assets[fileKey].path);
        fd.pipe(res);
      }
    }
  });
  next();
});
server.listen(port);
