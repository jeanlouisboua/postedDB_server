const http = require('http');
const eetase = require('eetase');
const socketClusterServer = require('socketcluster-server');
const express = require('express');
const serveStatic = require('serve-static');
const path = require('path');
const morgan = require('morgan');
const uuid = require('uuid');
const sccBrokerClient = require('scc-broker-client');

const ENVIRONMENT = process.env.ENV || 'dev';
const SOCKETCLUSTER_PORT = process.env.SOCKETCLUSTER_PORT || 8000;
const SOCKETCLUSTER_WS_ENGINE = process.env.SOCKETCLUSTER_WS_ENGINE || 'ws';
const SOCKETCLUSTER_SOCKET_CHANNEL_LIMIT = Number(process.env.SOCKETCLUSTER_SOCKET_CHANNEL_LIMIT) || 1000;
const SOCKETCLUSTER_LOG_LEVEL = process.env.SOCKETCLUSTER_LOG_LEVEL || 2;

const SCC_INSTANCE_ID = uuid.v4();
const SCC_STATE_SERVER_HOST = process.env.SCC_STATE_SERVER_HOST || null;
const SCC_STATE_SERVER_PORT = process.env.SCC_STATE_SERVER_PORT || null;
const SCC_MAPPING_ENGINE = process.env.SCC_MAPPING_ENGINE || null;
const SCC_CLIENT_POOL_SIZE = process.env.SCC_CLIENT_POOL_SIZE || null;
const SCC_AUTH_KEY = process.env.SCC_AUTH_KEY || null;
const SCC_INSTANCE_IP = process.env.SCC_INSTANCE_IP || null;
const SCC_INSTANCE_IP_FAMILY = process.env.SCC_INSTANCE_IP_FAMILY || null;
const SCC_STATE_SERVER_CONNECT_TIMEOUT = Number(process.env.SCC_STATE_SERVER_CONNECT_TIMEOUT) || null;
const SCC_STATE_SERVER_ACK_TIMEOUT = Number(process.env.SCC_STATE_SERVER_ACK_TIMEOUT) || null;
const SCC_STATE_SERVER_RECONNECT_RANDOMNESS = Number(process.env.SCC_STATE_SERVER_RECONNECT_RANDOMNESS) || null;
const SCC_PUB_SUB_BATCH_DURATION = Number(process.env.SCC_PUB_SUB_BATCH_DURATION) || null;
const SCC_BROKER_RETRY_DELAY = Number(process.env.SCC_BROKER_RETRY_DELAY) || null;

let agOptions = {};

if (process.env.SOCKETCLUSTER_OPTIONS) {
  let envOptions = JSON.parse(process.env.SOCKETCLUSTER_OPTIONS);
  Object.assign(agOptions, envOptions);
}

let httpServer = eetase(http.createServer());
let agServer = socketClusterServer.attach(httpServer, agOptions);

let expressApp = express();
expressApp.use(function (req, res, next) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "POST, PUT, OPTIONS, DELETE, GET");
  res.header("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
  next();
});

expressApp.use(express.json());


if (ENVIRONMENT === 'dev') {
  // Log every HTTP request. See https://github.com/expressjs/morgan for other
  // available formats.
  expressApp.use(morgan('dev'));
}
expressApp.use(serveStatic(path.resolve(__dirname, 'public')));

// Add GET /health-check express route
expressApp.get('/health-check', (req, res) => {
  res.status(200).send('OK');
});

/* API */
var socketIO;

expressApp.post("/create_database", (request, response) => {
  console.log("request receive !!!");
  var data = request.body;
  if (socketIO) {
    socketIO.transmit("create_database", data);
    (async () => {
      for await (let data of socketIO.receiver('create_database')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.post("/upgrade_database", (request, response) => {
  var data = request.body;
  if (socketIO) {
    socketIO.transmit("upgrade_database", data);
    (async () => {
      for await (let data of socketIO.receiver('upgrade_database')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.post("/populate_database", (request, response) => {
  var data = request.body;
  if (socketIO) {
    socketIO.transmit("populate_database", data);
    (async () => {
      for await (let data of socketIO.receiver('populate_database')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.get("/search_single_by_index", (request, response) => {
  var data = request.query;
  if (socketIO) {
    socketIO.transmit("search_single_by_index", data);
    (async () => {
      for await (let data of socketIO.receiver('search_single_by_index')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.get("/search_all_by_index", (request, response) => {
  var data = request.query;
  if (socketIO) {
    socketIO.transmit("search_all_by_index", data);
    (async () => {
      for await (let data of socketIO.receiver('search_all_by_index')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.get("/get_count", (request, response) => {
  var data = request.query;
  if (socketIO) {
    socketIO.transmit("get_count", data);
    (async () => {
      for await (let data of socketIO.receiver('get_count')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.get("/get_all_by_range", (request, response) => {
  var data = request.query;
  if (socketIO) {
    socketIO.transmit("get_all_by_range", data);
    (async () => {
      for await (let data of socketIO.receiver('get_all_by_range')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});

expressApp.get("/get_first_by_range", (request, response) => {
  var data = request.query;
  if (socketIO) {
    socketIO.transmit("get_first_by_range", data);
    (async () => {
      for await (let data of socketIO.receiver('get_first_by_range')) {
        return response.json(data);
      }
    })();
  } else {
    return response.sendStatus(700);
  }
});



// HTTP request handling loop.
(async () => {
  for await (let requestData of httpServer.listener('request')) {
    expressApp.apply(null, requestData);
  }
})();

// SocketCluster/WebSocket connection handling loop.
(async () => {
  for await (let { socket } of agServer.listener('connection')) {
    // Handle socket connection.
    console.log("new PostDB user" + socket.id + " connected !!!");
    socketIO = socket;
  }
})();

(async () => {
  for await (let { socket, code, reason } of agServer.listener('disconnection')) {
    console.log("user" + socket.id + "  disconnected...");
  }
})();

httpServer.listen(SOCKETCLUSTER_PORT);

if (SOCKETCLUSTER_LOG_LEVEL >= 1) {
  (async () => {
    for await (let { error } of agServer.listener('error')) {
      console.error(error);
    }
  })();
}

if (SOCKETCLUSTER_LOG_LEVEL >= 2) {
  console.log(
    `   ${colorText('[Active]', 32)} SocketCluster worker with PID ${process.pid} is listening on port ${SOCKETCLUSTER_PORT}`
  );

  (async () => {
    for await (let { warning } of agServer.listener('warning')) {
      console.warn(warning);
    }
  })();
}

function colorText(message, color) {
  if (color) {
    return `\x1b[${color}m${message}\x1b[0m`;
  }
  return message;
}

if (SCC_STATE_SERVER_HOST) {
  // Setup broker client to connect to SCC.
  let sccClient = sccBrokerClient.attach(agServer.brokerEngine, {
    instanceId: SCC_INSTANCE_ID,
    instancePort: SOCKETCLUSTER_PORT,
    instanceIp: SCC_INSTANCE_IP,
    instanceIpFamily: SCC_INSTANCE_IP_FAMILY,
    pubSubBatchDuration: SCC_PUB_SUB_BATCH_DURATION,
    stateServerHost: SCC_STATE_SERVER_HOST,
    stateServerPort: SCC_STATE_SERVER_PORT,
    mappingEngine: SCC_MAPPING_ENGINE,
    clientPoolSize: SCC_CLIENT_POOL_SIZE,
    authKey: SCC_AUTH_KEY,
    stateServerConnectTimeout: SCC_STATE_SERVER_CONNECT_TIMEOUT,
    stateServerAckTimeout: SCC_STATE_SERVER_ACK_TIMEOUT,
    stateServerReconnectRandomness: SCC_STATE_SERVER_RECONNECT_RANDOMNESS,
    brokerRetryDelay: SCC_BROKER_RETRY_DELAY
  });

  if (SOCKETCLUSTER_LOG_LEVEL >= 1) {
    (async () => {
      for await (let { error } of sccClient.listener('error')) {
        error.name = 'SCCError';
        console.error(error);
      }
    })();
  }
}
