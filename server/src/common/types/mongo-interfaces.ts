export interface IClientConnectionMongoConfig {
  connectionURL?: string;
  connectionMaxAttempts?: number;
  defaultMaxTimeMS?: number;
}

export interface IDbConfig {
  configConnectors: {
    [key: string]: {
      type: string;
      config: IClientConnectionMongoConfig;
    };
  };
  configRepositories: {
    tasks: string[];
  };
  configExcludeRepositories?: string[];
}

export interface IDBRepos {
  // reponame: repotype
}

export interface IRepositories extends IDBRepos {
  [key: string]: any;
}

export interface IDbWrapper {
  repositories: IRepositories;
}
