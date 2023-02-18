import {
  ObjectId,
  Db,
  MongoClient,
  UpdateQuery,
  UpdateOneOptions,
  FilterQuery,
  UpdateWriteOpResult,
  Collection,
} from "mongodb";
import { IClientConnectionMongoConfig } from "../types/mongo-interfaces";

export const TYPE = "mongo";
export default class MongoConnector {
  type = TYPE;

  private readonly _config: IClientConnectionMongoConfig;
  private readonly _defaultMaxTimeMS: number = 4000;
  private _mongoConnector: Promise<MongoClient> | null;

  constructor(dbConfig: IClientConnectionMongoConfig) {
    if (!dbConfig) {
      throw new Error("Validation error: Missing field: 'dbConfig'");
    }
    if (!dbConfig.connectionURL) {
      throw new Error(
        "Validation error: dbConfig is missing field: 'connectionURL'"
      );
    }

    this._config = dbConfig;
    this._defaultMaxTimeMS = this._config.defaultMaxTimeMS || 40000 * 60;

    this._mongoConnector = MongoClient.connect(this._config.connectionURL!, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
  }

  async getDbAndExecuteFunc(
    func: (db: Db, options: { defaultMaxTimeMS: number }) => Promise<any>,
    options?
  ): Promise<any> {
    const localOptions = options || {};

    const maxAttempts = this._config.connectionMaxAttempts || 3;
    let lastError: any = null;

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      try {
        if (this._mongoConnector) {
          const dbClient: MongoClient = await this._mongoConnector;
          return await func(dbClient.db(), {
            defaultMaxTimeMS: this._defaultMaxTimeMS,
          });
        }
      } catch (err: any) {
        console.error(`error executing query ${err?.message}`);

        if (
          (err.message &&
            err.message.toLowerCase() === "topology was destroyed") ||
          (err.name && err.name === "MongoNetworkError")
        ) {
          lastError = err;
          this._mongoConnector = MongoClient.connect(
            this._config.connectionURL!,
            { useNewUrlParser: true }
          );
          continue;
        }
        if (
          err.message &&
          err.message.toLowerCase() === "operation exceeded time limit"
        ) {
          lastError = err;
          continue;
        }
        throw err;
      }
    }

    console.warn(
      `MongoConnector. Exceed max attempts. Max attempts: ${maxAttempts}, operation: ${options.callerFuncName}`
    );
    throw lastError;
  }

  async getById(
    id: ObjectId,
    collectionName: string,
    props: any = {}
  ): Promise<any> {
    const localProps = props || {};
    const queryOptions: { projection?: any } = {};
    if (!id) {
      throw new Error("id parameter is missing");
    }
    if (!collectionName) {
      throw new Error("collection name is missing");
    }

    if (props.projection) {
      queryOptions.projection = props.projection;
    }

    return this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
      return dbRef.collection(collectionName).findOne(
        { _id: this.castToObjectId(id) },
        {
          maxTimeMS: localProps.maxTimeMS || defaultMaxTimeMS,
          ...queryOptions,
        }
      );
    }, `getById - ${collectionName}`);
  }

  async updateById(
    id: ObjectId,
    collectionName: string,
    updateBody: UpdateQuery<any>,
    updateOptions: UpdateOneOptions,
    props: any = {}
  ): Promise<UpdateWriteOpResult> {
    if (!id) {
      throw new Error("id parameter is missing");
    }
    if (!collectionName) {
      throw new Error("collection name is missing");
    }

    return await this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
      return dbRef
        .collection(collectionName)
        .updateOne({ _id: this.castToObjectId(id) }, updateBody, updateOptions);
    }, `getById - ${collectionName}`);
  }

  async updateOne(
    collectionName: string,
    query: FilterQuery<any>,
    updateBody: UpdateQuery<any>,
    updateOptions: UpdateOneOptions,
    props: any = {}
  ): Promise<UpdateWriteOpResult> {
    if (!collectionName) {
      throw new Error("collection name is missing");
    }

    // todo result type.
    return this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
      return dbRef
        .collection(collectionName)
        .updateOne(query, updateBody, updateOptions);
    }, `updateOne - ${collectionName}`);
  }

  public async getCollection(
    collectionName: string,
    props: any = {}
  ): Promise<Collection<any>> {
    if (!collectionName) {
      throw new Error("collection name is missing");
    }
    return (await this._mongoConnector).db().collection(collectionName);
  }

  /**
   * @return mongodb ObjectId
   * */
  castToObjectId(value: ObjectId, options: any = {}): ObjectId {
    return ObjectId.isValid(value) ? new ObjectId(value) : value;
  }

  /**
   * @return array of mongodb ObjectIds
   * */
  castArrayToObjectIds(ids: ObjectId[], options: any = {}): ObjectId[] {
    return ids.map((id) => {
      return ObjectId.isValid(id) ? new ObjectId(id) : id;
    });
  }

  async close(options: any): Promise<void> {
    try {
      const db = await this._mongoConnector;
      if (db) {
        await db.close();
      }
    } catch (err) {
      console.error(err);
    }
    this._mongoConnector = null;
  }
}

let dbWrapper: MongoConnector;

export async function connectMongoConnector() {
  if (!dbWrapper) {
    dbWrapper = new MongoConnector({
      connectionURL: process.env.MONGO_CONNECTION_STRING,
    });
  }
}

export function getDbWrapper(): MongoConnector {
  return dbWrapper;
}
