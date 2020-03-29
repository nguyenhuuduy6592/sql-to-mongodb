module.exports = {
    sqlConnectionString: "Server=.;Database=AdventureWorks2016_EXT;Trusted_Connection=Yes;Driver={SQL Server Native Client 11.0}",
    mongoConnectionString: "mongodb://localhost:27017", // This puts the resulting database in MongoDB running on your local PC.
    targetDatabaseName: "AdventureWorks2016_EXT", // Specify the MongoDB database where the data will end up.
    skip: [
        //"sql-table-to-skip-1", // Add the tables here that you don't want to replicate to MongoDB.
    ]
};