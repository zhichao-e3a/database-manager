const APP_MODE = "TEST"; // or "PROD"

if (!["TEST", "PROD"].includes(APP_MODE)) {
  throw new Error(`Invalid APP_MODE=${APP_MODE}`);
}

if (typeof MONGO_URI === "undefined") {
  throw new Error("MONGO_URI not provided. Load .env and pass via --eval.");
}

const DB_BY_MODE = {
  TEST: "Test",
  PROD: "Modoo_data",
};

const DB_NAME = DB_BY_MODE[APP_MODE];
const COLLECTION = "FILT_RECORDS";

print(`\n=== Mongo Schema Enforcement ===`);
print(`MODE : ${APP_MODE}`);
print(`URI  : ${MONGO_URI}`);
print(`DB   : ${DB_NAME}`);
print(`COLL : ${COLLECTION}\n`);

const conn = new Mongo(MONGO_URI);
const db = conn.getDB(DB_NAME);

const validator = {
    $jsonSchema: {
    bsonType: "object",
    required: [
        "_id",
        "mobile",
        "start_test_ts",
        "measurement_date",
        "uc",
        "fhr",
        "fmov",
        "gest_age",
        "origin",
        "sql_utime",
        "ctime",
        "utime",
        "doc_hash"
    ],
    properties: {
        _id             : { bsonType: ["int", "long"] },
        mobile          : { bsonType: "string" },
        start_test_ts   : { bsonType: "string" },
        measurement_date: { bsonType: "string" },
        uc: {
            bsonType: "array",
            items: { bsonType: "string" },
        },
        fhr: {
            bsonType: "array",
            items: { bsonType: "string" },
        },
        fmov: {
            bsonType: ["array", "null"],
            items: { bsonType: "string" },
        },
        gest_age        : { bsonType: ["int", "long"] },
        origin          : { bsonType: "string" },
        sql_utime       : { bsonType: "string" },
        ctime           : { bsonType: "string" },
        utime           : { bsonType: "string" },
        doc_hash        : { bsonType: "string" }
    },
    additionalProperties: false,
    },
};

const exists = db.getCollectionNames().includes(COLLECTION);

if (exists) {
  db.runCommand({
    collMod: COLLECTION,
    validator,
    validationLevel: "strict",
    validationAction: "error",
  });
  print(`Updated schema for ${DB_NAME}.${COLLECTION}`);
} else {
  db.createCollection(COLLECTION, {
    validator,
    validationLevel: "strict",
    validationAction: "error",
  });
  print(`Created collection ${DB_NAME}.${COLLECTION}`);
}
