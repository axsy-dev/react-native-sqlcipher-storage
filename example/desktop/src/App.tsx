import React, { useCallback, useRef, useReducer } from "react";
import {
  StyleSheet,
  Text,
  View,
  Button,
  FlatList,
  SafeAreaView
} from "react-native";

import SQLite from "react-native-sqlcipher-storage";
import type { Database, Transaction } from "react-native-sqlcipher-storage";

SQLite.DEBUG(true);
SQLite.enablePromise(true);

const database_name = "Test.db";
const database_key = "password";
const bad_database_key = "bad";

type State = {
  progress: string[];
};

type Actions =
  | { type: "addProgress"; text: string }
  | { type: "resetProgress" };

function reducer(state: State, action: Actions) {
  switch (action.type) {
    case "addProgress":
      return { progress: [...state.progress, action.text] };

    case "resetProgress":
      return { progress: [] };

    default:
      return state;
  }
}

export default function App() {
  const db = useRef<Database | null>(null);
  const [state, dispatch] = useReducer(reducer, { progress: [] });

  const addProgress = useCallback((text: string) => {
    console.log(text);
    dispatch({ type: "addProgress", text });
  }, []);

  const resetProgress = useCallback(() => {
    dispatch({ type: "resetProgress" });
  }, []);

  const addError = useCallback(
    (err: string) => {
      const errString = `Error: ${err}`;
      console.error(errString);
      addProgress(errString);
    },
    [addProgress]
  );

  const populateDB = useCallback(
    (tx: Transaction) => {
      addProgress("Executing DROP stmts");

      try {
        tx.executeSql("DROP TABLE IF EXISTS Employees;");
        tx.executeSql("DROP TABLE IF EXISTS Offices;");
        tx.executeSql("DROP TABLE IF EXISTS Departments;");
      } catch (e) {
        const err = e as Error;
        addError(err.message);
      }

      addProgress("Executing CREATE stmts");

      try {
        tx.executeSql(
          "CREATE TABLE IF NOT EXISTS Version( " +
            "version_id INTEGER PRIMARY KEY NOT NULL); "
        );
      } catch (e) {
        const err = e as Error;
        addError(err.message);
      }

      try {
        tx.executeSql(
          "CREATE TABLE IF NOT EXISTS Departments( " +
            "department_id INTEGER PRIMARY KEY NOT NULL, " +
            "name VARCHAR(30) ); "
        );
      } catch (e) {
        const err = e as Error;
        addError(err.message);
      }

      try {
        tx.executeSql(
          "CREATE TABLE IF NOT EXISTS Offices( " +
            "office_id INTEGER PRIMARY KEY NOT NULL, " +
            "name VARCHAR(20), " +
            "longtitude FLOAT, " +
            "latitude FLOAT ) ; "
        );
      } catch (e) {
        const err = e as Error;
        addError(err.message);
      }

      try {
        tx.executeSql(
          "CREATE TABLE IF NOT EXISTS Employees( " +
            "employe_id INTEGER PRIMARY KEY NOT NULL, " +
            "name VARCHAR(55), " +
            "office INTEGER, " +
            "department INTEGER, " +
            "FOREIGN KEY ( office ) REFERENCES Offices ( office_id ) " +
            "FOREIGN KEY ( department ) REFERENCES Departments ( department_id ));"
        );
      } catch (e) {
        const err = e as Error;
        addError(err.message);
      }

      addProgress("Executing INSERT stmts");

      tx.executeSql(
        'INSERT INTO Departments (name) VALUES ("Client Services");'
      );
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
      addProgress("all config SQL done");
    },
    [addProgress, addError]
  );

  const queryEmployees = useCallback(
    (tx: Transaction) => {
      addProgress("Executing employee query");
      tx.executeSql(
        "SELECT a.name, b.name as deptName FROM Employees a, Departments b WHERE a.department = b.department_id"
      )
        .then(([_tx, results]) => {
          console.log("tx results", results);
          var len = results.rows.length;
          for (let i = 0; i < len; i++) {
            let row = results.rows.item(i);
            addProgress(`Empl Name: ${row.name}, Dept Name: ${row.deptName}`);
          }
          addProgress("Query Employees completed");
        })
        .catch((error: Error) => {
          addError(error.message);
        });
    },
    [addProgress, addError]
  );

  const queryEmployeesNormal = useCallback(async () => {
    if (!db.current) return;

    addProgress("Running normal executeSql");

    try {
      const allResults = await db.current.executeSql(
        "SELECT a.name, b.name as deptName FROM Employees a, Departments b WHERE a.department = b.department_id"
      );
      console.log("results", allResults);

      const results = allResults[0];

      const len = results.rows.length;
      for (let i = 0; i < len; i++) {
        let row = results.rows.item(i);
        addProgress(`Empl Name: ${row.name}, Dept Name: ${row.deptName}`);
      }
      addProgress("Query Employees Normal completed");
    } catch (e) {
      const err = e as Error;
      addError(err.message);
    }
  }, [addProgress, addError]);

  const populateDatabase = useCallback(async () => {
    if (!db.current) return;

    addProgress("Populating database");

    try {
      await db.current.executeSql("SELECT 1 FROM Version LIMIT 1");
      addProgress("Database is ready ... executing query ...");

      db.current.transaction((tx: Transaction) => {
        queryEmployees(tx);
      });

      await queryEmployeesNormal();
    } catch (e) {
      console.log("Received error: ", e);
      addProgress("Database not yet ready ... populating data");
      db.current.transaction(async tx => {
        // Keep typescript happy
        if (!db.current) return;

        populateDB(tx);

        db.current.transaction((tx: Transaction) => {
          queryEmployees(tx);
          addProgress("Processing complete");
        });
      });
    }
  }, [addProgress, populateDB, queryEmployees, queryEmployeesNormal]);

  const loadAndQueryDB = useCallback(
    async (goodPassword: boolean) => {
      addProgress("Opening database....");

      try {
        console.log("SQlite", SQLite);
        const DB = await SQLite.openDatabase({
          name: database_name,
          key: goodPassword ? database_key : bad_database_key
        });

        db.current = DB;

        addProgress("Database OPEN");
        await populateDatabase();
      } catch (e: unknown) {
        const err = e as Error;
        addError(err.message);
      }
    },
    [addProgress, populateDatabase, addError]
  );

  const runDemo = useCallback(async () => {
    resetProgress();
    addProgress("Starting SQLite Demo");
    await loadAndQueryDB(true);
  }, [addProgress, loadAndQueryDB, resetProgress]);

  const closeDatabase = useCallback(async () => {
    if (db.current) {
      try {
        await db.current.close();
        db.current = null;
        addProgress("Database Closed");
      } catch (e: unknown) {
        const error = e as Error;
        addError(error.message);
      }
    }
  }, [addError, addProgress]);

  const deleteDatabase = useCallback(async () => {
    resetProgress();
    try {
      addProgress("Deleting Database");
      await SQLite.deleteDatabase(database_name);
      addProgress("Database Deleted");
    } catch (e: unknown) {
      const error = e as Error;
      addError(error.message);
    }
  }, [addError, addProgress, resetProgress]);

  const runBadPwd = useCallback(async () => {
    resetProgress();
    addProgress("Trying to open with bad password");

    await closeDatabase();
    await loadAndQueryDB(false);
  }, [addProgress, closeDatabase, loadAndQueryDB, resetProgress]);

  const migrateDb = useCallback(async () => {
    resetProgress();
    addProgress("migrating database");
    try {
      const DB = await SQLite.openDatabase({
        name: "2x.db",
        createFromLocation: 1,
        key: "test"
      });
      addProgress("migrated database");
      await DB.close();
      addProgress("closed database");
      await SQLite.deleteDatabase("2x.db");
      addProgress("deleted database");
    } catch (e) {
      const err = e as Error;
      addError(err.message);
    }
  }, [addError, addProgress, resetProgress]);

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
        <Button title="Bad Password (should error)" onPress={runBadPwd} />
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

const listStyles = StyleSheet.create({
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

const styles = StyleSheet.create({
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
