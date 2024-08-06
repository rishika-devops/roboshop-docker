const mongoClient = require('mongodb').MongoClient;
const mongoObjectID = require('mongodb').ObjectID;
const bodyParser = require('body-parser');
const express = require('express');
const pino = require('pino');
const expPino = require('express-pino-logger');

const logger = pino({
    level: 'info',
    prettyPrint: false,
    useLevelLabels: true
});
const expLogger = expPino({
    logger: logger
});

// MongoDB
let db;
let collection;
let mongoConnected = false;

const app = express();

app.use(expLogger);

app.use((req, res, next) => {
    res.set('Timing-Allow-Origin', '*');
    res.set('Access-Control-Allow-Origin', '*');
    next();
});

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.get('/health', (req, res) => {
    const stat = {
        app: 'OK',
        mongo: mongoConnected
    };
    res.json(stat);
});

// All products
app.get('/products', (req, res) => {
    if (mongoConnected) {
        collection.find({}).toArray().then((products) => {
            res.json(products);
        }).catch((e) => {
            req.log.error('ERROR', e);
            res.status(500).send(e);
        });
    } else {
        req.log.error('Database not available');
        res.status(500).send('Database not available');
    }
});

// Product by SKU
app.get('/product/:sku', (req, res) => {
    if (mongoConnected) {
        const delay = process.env.GO_SLOW || 0;
        setTimeout(() => {
            collection.findOne({ sku: req.params.sku }).then((product) => {
                req.log.info('product', product);
                if (product) {
                    res.json(product);
                } else {
                    res.status(404).send('SKU not found');
                }
            }).catch((e) => {
                req.log.error('ERROR', e);
                res.status(500).send(e);
            });
        }, delay);
    } else {
        req.log.error('Database not available');
        res.status(500).send('Database not available');
    }
});

// Products in a category
app.get('/products/:cat', (req, res) => {
    if (mongoConnected) {
        collection.find({ categories: req.params.cat }).sort({ name: 1 }).toArray().then((products) => {
            if (products) {
                res.json(products);
            } else {
                res.status(404).send('No products for ' + req.params.cat);
            }
        }).catch((e) => {
            req.log.error('ERROR', e);
            res.status(500).send(e);
        });
    } else {
        req.log.error('Database not available');
        res.status(500).send('Database not available');
    }
});

// All categories
app.get('/categories', (req, res) => {
    if (mongoConnected) {
        collection.distinct('categories').then((categories) => {
            res.json(categories);
        }).catch((e) => {
            req.log.error('ERROR', e);
            res.status(500).send(e);
        });
    } else {
        req.log.error('Database not available');
        res.status(500).send('Database not available');
    }
});

// Search name and description
app.get('/search/:text', (req, res) => {
    if (mongoConnected) {
        collection.find({ '$text': { '$search': req.params.text } }).toArray().then((hits) => {
            res.json(hits);
        }).catch((e) => {
            req.log.error('ERROR', e);
            res.status(500).send(e);
        });
    } else {
        req.log.error('Database not available');
        res.status(500).send('Database not available');
    }
});

// Unified MongoDB connection function
function mongoConnect() {
    return new Promise((resolve, reject) => {
        let mongoURL;

        if (process.env.MONGO === 'true') {
            mongoURL = process.env.MONGO_URL || 'mongodb://mongodb:27017/catalogue';
        } else if (process.env.DOCUMENTDB === 'true') {
            mongoURL = process.env.MONGO_URL || 'mongodb://username:password@mongodb:27017/catalogue?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false';
        } else {
            return reject('No database configuration set');
        }

        mongoClient.connect(mongoURL, { useNewUrlParser: true, useUnifiedTopology: true })
            .then(client => {
                db = client.db('catalogue');
                collection = db.collection('products');
                resolve('connected');
            })
            .catch(error => reject(error));
    });
}

// MongoDB connection retry loop
function mongoLoop() {
    mongoConnect().then((r) => {
        mongoConnected = true;
        logger.info('MongoDB connected');
    }).catch((e) => {
        logger.error('ERROR', e);
        setTimeout(mongoLoop, 2000);
    });
}

mongoLoop();

// Fire it up!
const port = process.env.CATALOGUE_SERVER_PORT || '8080';
app.listen(port, () => {
    logger.info('Started on port', port);
});
