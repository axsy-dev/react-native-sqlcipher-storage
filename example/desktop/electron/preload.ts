// See the Electron documentation for details on how to use preload scripts:
// https://www.electronjs.org/docs/latest/tutorial/process-model#preload-scripts
import { db } from "react-native-sqlcipher-storage/preload";

db.preload.init();
