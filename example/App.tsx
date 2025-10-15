/*
 * sqlite.ios.promise.js
 *
 * Created by Andrzej Porebski on 10/29/15.
 * Copyright (c) 2015 Andrzej Porebski.
 *
 * Test App using Promise for react-naive-sqlite-storage
 *
 * This library is available under the terms of the MIT License (2008).
 * See http://opensource.org/licenses/alphabetical for full text.
 */
"use strict";

import SQLite, { Database } from "react-native-sqlcipher-storage";

SQLite.DEBUG(true);
SQLite.enablePromise(true);

import React, { useCallback, useRef, useReducer } from "react";
import {
  StyleSheet,
  Text,
  View,
  Button,
  FlatList,
  SafeAreaView
} from "react-native";
import Sqlite from "react-native-sqlcipher-storage";

const database_name = "Test.db";
const database_key = "password";
const bad_database_key = "bad";

type State = {
  progress: string[];
};

type Actions = { type: "addProgress"; text: string };

function reducer(state: State, action: Actions) {
  if (action.type === "addProgress") {
    return { progress: [...state.progress, action.text] };
  }
  return state;
}

let db: Database | null = null;

function App() {
  // const db = useRef<Database | null>(null);
  const [state, dispatch] = useReducer(reducer, { progress: [] });

  const addProgress = useCallback((text: string) => {
    dispatch({ type: "addProgress", text });
  }, []);

  const addError = useCallback(
    (err: string) => {
      addProgress(`Error: ${err}`);
    },
    [addProgress]
  );

  const populateDatabase = useCallback(async () => {
    addProgress("Populating database");
    return Promise.resolve();
  }, [addProgress]);

  const loadAndQueryDB = useCallback(
    async (goodPassword: boolean) => {
      addProgress("Opening database....");

      try {
        const DB = await SQLite.openDatabase({
          name: database_name,
          key: goodPassword ? database_key : bad_database_key
        });

        db = DB;

        addProgress("Database OPEN");
        await populateDatabase();
      } catch (e: unknown) {
        const err = e as Error;
        console.log(e);
        addError(err.message);
      }
    },
    [addProgress, populateDatabase, addError]
  );

  const runDemo = useCallback(async () => {
    addProgress("Starting SQLite Demo");
    await loadAndQueryDB(true);
  }, [addProgress, loadAndQueryDB]);

  const closeDatabase = useCallback(async () => {
    if (db) {
      try {
        await db.close();
        db = null;
        addProgress("Database Closed");
      } catch (e: unknown) {
        const error = e as Error;
        addError(error.message);
      }
    }
  }, [addError, addProgress]);

  const deleteDatabase = useCallback(async () => {
    try {
      addProgress("Deleting Database");
      await Sqlite.deleteDatabase(database_name);
      addProgress("Database Deleted");
    } catch (e: unknown) {
      const error = e as Error;
      addError(error.message);
    }
  }, [addError, addProgress]);

  const runBadPwd = useCallback(() => {}, []);
  const migrateDb = useCallback(() => {}, []);
  const renderProgressEntry = useCallback(({ item }: { item: string }) => {
    return (
      <View style={listStyles.li}>
        <View>
          <Text
            style={[
              listStyles.liText,
              item.startsWith("Error") ? listStyles.liErrorText : {}
            ]}
          >
            {item}
          </Text>
        </View>
      </View>
    );
  }, []);

  React.useEffect(() => {
    return () => {
      closeDatabase();
    };
  }, [closeDatabase]);

  return (
    <SafeAreaView style={styles.mainContainer}>
      <View>
        <Button title="Run tests" onPress={runDemo} />
        <Button title="Close DB" onPress={closeDatabase} />
        <Button title="Delete DB" onPress={deleteDatabase} />
        <Button title="Bad Password" onPress={runBadPwd} />
        <Button title="Migrate Test" onPress={migrateDb} />
      </View>
      <FlatList
        data={state.progress}
        renderItem={renderProgressEntry}
        style={listStyles.liContainer}
      />
    </SafeAreaView>
  );
}

/*
class App extends React.Component<Props, State> {
  state = {
    progress: []
  };

  componentWillUnmount() {
    this.closeDatabase();
  }

  errorCB = (err: string) => {
    console.log("error: ", err);
    const { progress } = this.state;
    progress.push(`Error : ${err}`);
    this.setState({ progress });
  };

  addProgress = (text: string) => {
    let { progress } = this.state;
    this.setState({ progress: [...progress, text] });
  };

  populateDatabase = async (db: Database) => {
    this.addProgress("Database integrity check");

    try {
      await db.executeSql("SELECT 1 FROM Version LIMIT 1");
      this.addProgress("Database is ready ... executing query ...");

      db.transaction(async tx => {
        await this.queryEmployees(tx);
        this.addProgress("Processing completed");
      });
    } catch (e) {
      console.log("Received error: ", e);
      this.addProgress("Database not yet ready ... populating data");
      db.transaction(async tx => {
        await this.populateDB(tx);
        db.transaction(async tx => {
          await this.queryEmployees(tx);
          this.addProgress("Processing complete");
        });
      });
    }

    //   .then(() => {
    //     this.addProgress("Database is ready ... executing query ...");

    //     db.transaction(this.queryEmployees).then(() => {
    //       this.addProgress("Processing completed");
    //     });
    //   })
    //   .catch(error => {
    //     console.log("Received error: ", error);
    //     this.addProgress("Database not yet ready ... populating data");
    //     db.transaction(this.populateDB).then(() => {
    //       this.addProgress("Database populated ... executing query ...");
    //       db.transaction(this.queryEmployees).then(result => {
    //         this.addProgress("Processing completed");
    //       });
    //     });
    //   });
  };

  populateDB = tx => {
    this.addProgress("Executing DROP stmts");

    tx.executeSql("DROP TABLE IF EXISTS Employees;");
    tx.executeSql("DROP TABLE IF EXISTS Offices;");
    tx.executeSql("DROP TABLE IF EXISTS Departments;");

    this.addProgress("Executing CREATE stmts");

    tx.executeSql(
      "CREATE TABLE IF NOT EXISTS Version( " +
        "version_id INTEGER PRIMARY KEY NOT NULL); "
    ).catch((error: Error) => {
      this.errorCB(error.message);
    });

    tx.executeSql(
      "CREATE TABLE IF NOT EXISTS Departments( " +
        "department_id INTEGER PRIMARY KEY NOT NULL, " +
        "name VARCHAR(30) ); "
    ).catch((error: Error) => {
      this.errorCB(error.message);
    });

    tx.executeSql(
      "CREATE TABLE IF NOT EXISTS Offices( " +
        "office_id INTEGER PRIMARY KEY NOT NULL, " +
        "name VARCHAR(20), " +
        "longtitude FLOAT, " +
        "latitude FLOAT ) ; "
    ).catch((error: Error) => {
      this.errorCB(error.message);
    });

    tx.executeSql(
      "CREATE TABLE IF NOT EXISTS Employees( " +
        "employe_id INTEGER PRIMARY KEY NOT NULL, " +
        "name VARCHAR(55), " +
        "office INTEGER, " +
        "department INTEGER, " +
        "FOREIGN KEY ( office ) REFERENCES Offices ( office_id ) " +
        "FOREIGN KEY ( department ) REFERENCES Departments ( department_id ));"
    ).catch((error: Error) => {
      this.errorCB(error.message);
    });

    this.addProgress("Executing INSERT stmts");

    tx.executeSql('INSERT INTO Departments (name) VALUES ("Client Services");');
    tx.executeSql(
      'INSERT INTO Departments (name) VALUES ("Investor Services");'
    );
    tx.executeSql('INSERT INTO Departments (name) VALUES ("Shipping");');
    tx.executeSql('INSERT INTO Departments (name) VALUES ("Direct Sales");');

    tx.executeSql(
      'INSERT INTO Offices (name, longtitude, latitude) VALUES ("Denver", 59.8,  34.1);'
    );
    tx.executeSql(
      'INSERT INTO Offices (name, longtitude, latitude) VALUES ("Warsaw", 15.7, 54.1);'
    );
    tx.executeSql(
      'INSERT INTO Offices (name, longtitude, latitude) VALUES ("Berlin", 35.3, 12.1);'
    );
    tx.executeSql(
      'INSERT INTO Offices (name, longtitude, latitude) VALUES ("Paris", 10.7, 14.1);'
    );

    tx.executeSql(
      "INSERT INTO Employees (name, office, department) VALUES (?,?,?);",
      ["Sylvester Stallone", 2, 4]
    );
    tx.executeSql(
      "INSERT INTO Employees (name, office, department) VALUES (?,?,?);",
      ["Elvis Presley", 2, 4]
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Leslie Nelson", 3,  4);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Fidel Castro", 3, 3);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Bill Clinton", 1, 3);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Margaret thischer", 1, 3);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Donald Trump", 1, 3);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Dr DRE", 2, 2);'
    );
    tx.executeSql(
      'INSERT INTO Employees (name, office, department) VALUES ("Samantha Fox", 2, 1);'
    );
    this.addProgress("all config SQL done");
  };

  queryEmployees = tx => {
    console.log("Executing employee query");
    tx.executeSql(
      "SELECT a.name, b.name as deptName FROM Employees a, Departments b WHERE a.department = b.department_id"
    )
      .then(([tx, results]) => {
        this.addProgress("Query completed");
        var len = results.rows.length;
        for (let i = 0; i < len; i++) {
          let row = results.rows.item(i);
          this.addProgress(
            `Empl Name: ${row.name}, Dept Name: ${row.deptName}`
          );
        }
      })
      .catch((error: Error) => {
        console.log(error);
      });
  };

  loadAndQueryDB = async (goodPassword: boolean) => {
    this.addProgress("Opening database ...");
    this.addProgress(goodPassword ? "Good Password" : "Bad Password");

    try {
      const DB = await SQLite.openDatabase({
        name: database_name,
        key: goodPassword ? database_key : bad_database_key
      });

      db = DB;

      this.addProgress("Database OPEN");
      this.populateDatabase(DB);
    } catch (e) {
      this.addProgress("Database failed to open");
    }
  };

  closeDb = async () => {
    try {
      await db.close();
      this.addProgress("Database CLOSED");
    } catch (error) {
      this.errorCB(error.message);
    }
  };

  closeDatabase = () => {
    if (db) {
      this.setState({ progress: ["Closing DB"] }, this.closeDb);
    } else {
      this.addProgress("Database was not OPENED");
    }
  };

  deleteDatabase = () => {
    this.setState({ progress: ["Deleting database"] }, async () => {
      try {
        await SQLite.deleteDatabase(database_name);
        this.addProgress("Database DELETED");
      } catch (error) {
        this.errorCB(error.message);
      }
    });
  };

  runDemo = () => {
    const progress = ["Starting SQLite Demo"];
    this.setState({ progress }, async () => {
      await this.loadAndQueryDB(true);
    });
  };

  runBadPwd = () => {
    this.setState(
      { progress: ["Trying to open with bad password"] },
      async () => {
        await this.closeDb();
        await this.loadAndQueryDB(false);
      }
    );
  };

  renderProgressEntry = ({ item }: { item: string }) => {
    return (
      <View style={listStyles.li}>
        <View>
          <Text style={listStyles.liText}>{item}</Text>
        </View>
      </View>
    );
  };

  migrateDb = async () => {
    try {
      const DB = await SQLite.openDatabase({
        name: "2x.db",
        createFromLocation: 1,
        key: "test"
      });
      this.addProgress("Migrated database");
      await DB.close();
      this.addProgress("Closed database");
      await SQLite.deleteDatabase("2x.db");
      this.addProgress("Deleted Database");
    } catch (err) {
      this.addProgress("Database migration failed");
    }
  };

  render() {
    return (
      <SafeAreaView style={styles.mainContainer}>
        <View>
          <Button title="Run tests" onPress={this.runDemo} />
          <Button title="Close DB" onPress={this.closeDatabase} />
          <Button title="Delete DB" onPress={this.deleteDatabase} />
          <Button title="Bad Password" onPress={this.runBadPwd} />
          <Button title="Migrate Test" onPress={this.migrateDb} />
        </View>
        <FlatList
          data={this.state.progress}
          renderItem={this.renderProgressEntry}
          style={listStyles.liContainer}
        />
      </SafeAreaView>
    );
  }
}
*/
export default App;

var listStyles = StyleSheet.create({
  li: {
    borderBottomColor: "#c8c7cc",
    borderBottomWidth: 0.5,
    paddingTop: 15,
    paddingRight: 15,
    paddingBottom: 15
  },
  liContainer: {
    backgroundColor: "#fff",
    flex: 1,
    paddingLeft: 15
  },
  liIndent: {
    flex: 1
  },
  liText: {
    color: "#333",
    fontSize: 17,
    fontWeight: "400",
    marginBottom: -3.5,
    marginTop: -3.5
  },
  liErrorText: {
    color: "red",
    fontSize: 17,
    fontWeight: "400",
    marginBottom: -3.5,
    marginTop: -3.5
  }
});

var styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#F5FCFF"
  },
  welcome: {
    fontSize: 20,
    textAlign: "center",
    margin: 10
  },
  instructions: {
    textAlign: "center",
    color: "#333333",
    marginBottom: 5
  },
  toolbar: {
    backgroundColor: "#51c04d",
    paddingTop: 100,
    paddingBottom: 10,
    flexDirection: "row",
    justifyContent: "center"
  },
  toolbarButton: {
    color: "blue",
    textAlign: "center",
    flex: 1
  },
  toolbarTouchable: {
    flex: 1
  },
  mainContainer: {
    flex: 1
  }
});
