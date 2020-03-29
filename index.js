"use strict";

const mongodb = require('mongodb');
const sql = require('mssql/msnodesqlv8');
const E = require('linq');

const config = require("./config.js");

//
// Replicate an entire SQL table to MongoDB.
//
async function replicateTable (tableName, primaryKeyField, targetDb, sqlPool, config) {

    console.log("Replicating " + tableName + " with primary key " + primaryKeyField);

    const collection = targetDb.collection(tableName);
    
    const query = "select * from " + tableName;
    console.log("Executing query: " + query);
    const tableResult = await sqlPool.request().query(query);

    console.log("Got " + tableResult.recordset.length + " records from table " + tableName);

    if (tableResult.recordset.length === 0) {
        console.log('No records to transfer.');
        return;
    }

    const primaryKeyRemap = [];

    const bulkRecordInsert = E.from(tableResult.recordset)
        .select(row => {
            row._id = new mongodb.ObjectID();   
            return {
                insertOne: {
                    document: row
                },
            }            
        })
        .toArray();

    await collection.bulkWrite(bulkRecordInsert);
};

//
// Remap foreign keys for a MongoBD collection
//
async function remapForeignKeys (tableName, foreignKeysMap, targetDb, sqlPool) {

    if (!foreignKeysMap) {
        console.log(tableName + " has no foreign keys.");        
        return;
    }

    const foreignKeys = Object.keys(foreignKeysMap);
    if (foreignKeys.length ==- 0) {
        console.log(tableName + " has no foreign keys.");
        return;
    }

    console.log("Remapping foreign keys for " + tableName);
    
    const thisCollection = targetDb.collection(tableName);
    const records = await thisCollection.find().toArray();
    console.log("Updating " + records.length + " records.");

    for (const record of records) {
        const foreignKeyUpdates = {};
        let updatesMade = false;

        for (const foreignKey of foreignKeys) {
            if (!record[foreignKey]) {
                // No value.
                continue;
            }
            const otherTableName = foreignKeysMap[foreignKey].table;
            const otherTableRemap = targetDb.collection(otherTableName + '-pkremap');
            const remap = await otherTableRemap.findOne({ _id: record[foreignKey] });
            foreignKeyUpdates[foreignKey] = remap.new;
            updatesMade = true;
        }

        if (!updatesMade) {
            continue;
        }

        thisCollection.update({ _id: record._id }, { $set: foreignKeyUpdates });
    }
}

async function main () {

    const mongoClient = await mongodb.MongoClient.connect(config.mongoConnectionString);
    const targetDb = mongoClient.db(config.targetDatabaseName);

    const sqlPool = await new sql.ConnectionPool(config.sqlConnectionString).connect();
   
    const primaryKeysQuery = `
        SELECT TABLE_NAME = A.TABLE_SCHEMA + '.' + A.TABLE_NAME
            , A.CONSTRAINT_NAME
            , B.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS A
        , INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE B
        WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
        ORDER BY A.TABLE_NAME
    `;
    const primaryKeysResult = await sqlPool.request().query(primaryKeysQuery);
    const primaryKeyMap = E.from(primaryKeysResult.recordset)
        .toObject(
            row => row.TABLE_NAME,
            row => row.COLUMN_NAME
        );

    const primaryKeysCollection = targetDb.collection("primaryKeys");
    await primaryKeysCollection.insertMany(primaryKeysResult.recordset);

    const tablesResult = await sqlPool.request().query(`
        SELECT TABLE_CATALOG
            , TABLE_SCHEMA
            , TABLE_NAME = TABLE_SCHEMA + '.' + TABLE_NAME
            , TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
    `);
    const tableNames = E.from(tablesResult.recordset)
        .select(row => row.TABLE_NAME)
        .where(tableName => config.skip.indexOf(tableName) === -1)
        .distinct()
        .toArray();

    console.log("Replicating SQL tables " + tableNames.join(', '));
    console.log("It's time for a coffee or three.");

    for (const tableName of tableNames) {
        await replicateTable(tableName, primaryKeyMap[tableName], targetDb, sqlPool, config);    
    }

    await sqlPool.close();
    await mongoClient.close();
}

main()
    .then(() => {
        console.log('Done');
    })
    .catch(err => {
        console.error("Database replication errored out.");
        console.error(err);
    });

