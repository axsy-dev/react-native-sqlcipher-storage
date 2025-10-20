/******/ (() => { // webpackBootstrap
/******/ 	var __webpack_modules__ = ({

/***/ "./node_modules/electron-squirrel-startup/index.js":
/*!*********************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/index.js ***!
  \*********************************************************/
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var path=__webpack_require__(/*! path */ "path");var spawn=(__webpack_require__(/*! child_process */ "child_process").spawn);var debug=__webpack_require__(/*! debug */ "./node_modules/electron-squirrel-startup/node_modules/debug/src/index.js")('electron-squirrel-startup');var app=(__webpack_require__(/*! electron */ "electron").app);var run=function run(args,done){var updateExe=path.resolve(path.dirname(process.execPath),'..','Update.exe');debug('Spawning `%s` with args `%s`',updateExe,args);spawn(updateExe,args,{detached:true}).on('close',done);};var check=function check(){if(process.platform==='win32'){var cmd=process.argv[1];debug('processing squirrel command `%s`',cmd);var target=path.basename(process.execPath);if(cmd==='--squirrel-install'||cmd==='--squirrel-updated'){run(['--createShortcut='+target+''],app.quit);return true;}if(cmd==='--squirrel-uninstall'){run(['--removeShortcut='+target+''],app.quit);return true;}if(cmd==='--squirrel-obsolete'){app.quit();return true;}}return false;};module.exports=check();

/***/ }),

/***/ "./node_modules/electron-squirrel-startup/node_modules/debug/src/browser.js":
/*!**********************************************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/node_modules/debug/src/browser.js ***!
  \**********************************************************************************/
/***/ ((module, exports, __webpack_require__) => {

exports=module.exports = __webpack_require__(/*! ./debug */ "./node_modules/electron-squirrel-startup/node_modules/debug/src/debug.js");exports.log=log;exports.formatArgs=formatArgs;exports.save=save;exports.load=load;exports.useColors=useColors;exports.storage='undefined'!=typeof chrome&&'undefined'!=typeof chrome.storage?chrome.storage.local:localstorage();exports.colors=['lightseagreen','forestgreen','goldenrod','dodgerblue','darkorchid','crimson'];function useColors(){if(typeof window!=='undefined'&&window.process&&window.process.type==='renderer'){return true;}return typeof document!=='undefined'&&document.documentElement&&document.documentElement.style&&document.documentElement.style.WebkitAppearance||typeof window!=='undefined'&&window.console&&(window.console.firebug||window.console.exception&&window.console.table)||typeof navigator!=='undefined'&&navigator.userAgent&&navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/)&&parseInt(RegExp.$1,10)>=31||typeof navigator!=='undefined'&&navigator.userAgent&&navigator.userAgent.toLowerCase().match(/applewebkit\/(\d+)/);}exports.formatters.j=function(v){try{return JSON.stringify(v);}catch(err){return'[UnexpectedJSONParseError]: '+err.message;}};function formatArgs(args){var useColors=this.useColors;args[0]=(useColors?'%c':'')+this.namespace+(useColors?' %c':' ')+args[0]+(useColors?'%c ':' ')+'+'+exports.humanize(this.diff);if(!useColors)return;var c='color: '+this.color;args.splice(1,0,c,'color: inherit');var index=0;var lastC=0;args[0].replace(/%[a-zA-Z%]/g,function(match){if('%%'===match)return;index++;if('%c'===match){lastC=index;}});args.splice(lastC,0,c);}function log(){return'object'===typeof console&&console.log&&Function.prototype.apply.call(console.log,console,arguments);}function save(namespaces){try{if(null==namespaces){exports.storage.removeItem('debug');}else{exports.storage.debug=namespaces;}}catch(e){}}function load(){var r;try{r=exports.storage.debug;}catch(e){}if(!r&&typeof process!=='undefined'&&'env'in process){r=process.env.DEBUG;}return r;}exports.enable(load());function localstorage(){try{return window.localStorage;}catch(e){}}

/***/ }),

/***/ "./node_modules/electron-squirrel-startup/node_modules/debug/src/debug.js":
/*!********************************************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/node_modules/debug/src/debug.js ***!
  \********************************************************************************/
/***/ ((module, exports, __webpack_require__) => {

exports=module.exports=createDebug.debug=createDebug['default']=createDebug;exports.coerce=coerce;exports.disable=disable;exports.enable=enable;exports.enabled=enabled;exports.humanize = __webpack_require__(/*! ms */ "./node_modules/electron-squirrel-startup/node_modules/ms/index.js");exports.names=[];exports.skips=[];exports.formatters={};var prevTime;function selectColor(namespace){var hash=0,i;for(i in namespace){hash=(hash<<5)-hash+namespace.charCodeAt(i);hash|=0;}return exports.colors[Math.abs(hash)%exports.colors.length];}function createDebug(namespace){function debug(){if(!debug.enabled)return;var self=debug;var curr=+new Date();var ms=curr-(prevTime||curr);self.diff=ms;self.prev=prevTime;self.curr=curr;prevTime=curr;var args=new Array(arguments.length);for(var i=0;i<args.length;i++){args[i]=arguments[i];}args[0]=exports.coerce(args[0]);if('string'!==typeof args[0]){args.unshift('%O');}var index=0;args[0]=args[0].replace(/%([a-zA-Z%])/g,function(match,format){if(match==='%%')return match;index++;var formatter=exports.formatters[format];if('function'===typeof formatter){var val=args[index];match=formatter.call(self,val);args.splice(index,1);index--;}return match;});exports.formatArgs.call(self,args);var logFn=debug.log||exports.log||console.log.bind(console);logFn.apply(self,args);}debug.namespace=namespace;debug.enabled=exports.enabled(namespace);debug.useColors=exports.useColors();debug.color=selectColor(namespace);if('function'===typeof exports.init){exports.init(debug);}return debug;}function enable(namespaces){exports.save(namespaces);exports.names=[];exports.skips=[];var split=(typeof namespaces==='string'?namespaces:'').split(/[\s,]+/);var len=split.length;for(var i=0;i<len;i++){if(!split[i])continue;namespaces=split[i].replace(/\*/g,'.*?');if(namespaces[0]==='-'){exports.skips.push(new RegExp('^'+namespaces.substr(1)+'$'));}else{exports.names.push(new RegExp('^'+namespaces+'$'));}}}function disable(){exports.enable('');}function enabled(name){var i,len;for(i=0,len=exports.skips.length;i<len;i++){if(exports.skips[i].test(name)){return false;}}for(i=0,len=exports.names.length;i<len;i++){if(exports.names[i].test(name)){return true;}}return false;}function coerce(val){if(val instanceof Error)return val.stack||val.message;return val;}

/***/ }),

/***/ "./node_modules/electron-squirrel-startup/node_modules/debug/src/index.js":
/*!********************************************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/node_modules/debug/src/index.js ***!
  \********************************************************************************/
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

if(typeof process!=='undefined'&&process.type==='renderer'){module.exports = __webpack_require__(/*! ./browser.js */ "./node_modules/electron-squirrel-startup/node_modules/debug/src/browser.js");}else{module.exports = __webpack_require__(/*! ./node.js */ "./node_modules/electron-squirrel-startup/node_modules/debug/src/node.js");}

/***/ }),

/***/ "./node_modules/electron-squirrel-startup/node_modules/debug/src/node.js":
/*!*******************************************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/node_modules/debug/src/node.js ***!
  \*******************************************************************************/
/***/ ((module, exports, __webpack_require__) => {

var tty=__webpack_require__(/*! tty */ "tty");var util=__webpack_require__(/*! util */ "util");exports=module.exports = __webpack_require__(/*! ./debug */ "./node_modules/electron-squirrel-startup/node_modules/debug/src/debug.js");exports.init=init;exports.log=log;exports.formatArgs=formatArgs;exports.save=save;exports.load=load;exports.useColors=useColors;exports.colors=[6,2,3,4,5,1];exports.inspectOpts=Object.keys(process.env).filter(function(key){return /^debug_/i.test(key);}).reduce(function(obj,key){var prop=key.substring(6).toLowerCase().replace(/_([a-z])/g,function(_,k){return k.toUpperCase();});var val=process.env[key];if(/^(yes|on|true|enabled)$/i.test(val))val=true;else if(/^(no|off|false|disabled)$/i.test(val))val=false;else if(val==='null')val=null;else val=Number(val);obj[prop]=val;return obj;},{});var fd=parseInt(process.env.DEBUG_FD,10)||2;if(1!==fd&&2!==fd){util.deprecate(function(){},'except for stderr(2) and stdout(1), any other usage of DEBUG_FD is deprecated. Override debug.log if you want to use a different log function (https://git.io/debug_fd)')();}var stream=1===fd?process.stdout:2===fd?process.stderr:createWritableStdioStream(fd);function useColors(){return'colors'in exports.inspectOpts?Boolean(exports.inspectOpts.colors):tty.isatty(fd);}exports.formatters.o=function(v){this.inspectOpts.colors=this.useColors;return util.inspect(v,this.inspectOpts).split('\n').map(function(str){return str.trim();}).join(' ');};exports.formatters.O=function(v){this.inspectOpts.colors=this.useColors;return util.inspect(v,this.inspectOpts);};function formatArgs(args){var name=this.namespace;var useColors=this.useColors;if(useColors){var c=this.color;var prefix="  \x1B[3"+c+';1m'+name+' '+"\x1B[0m";args[0]=prefix+args[0].split('\n').join('\n'+prefix);args.push("\x1B[3"+c+'m+'+exports.humanize(this.diff)+"\x1B[0m");}else{args[0]=new Date().toUTCString()+' '+name+' '+args[0];}}function log(){return stream.write(util.format.apply(util,arguments)+'\n');}function save(namespaces){if(null==namespaces){delete process.env.DEBUG;}else{process.env.DEBUG=namespaces;}}function load(){return process.env.DEBUG;}function createWritableStdioStream(fd){var stream;var tty_wrap=process.binding('tty_wrap');switch(tty_wrap.guessHandleType(fd)){case'TTY':stream=new tty.WriteStream(fd);stream._type='tty';if(stream._handle&&stream._handle.unref){stream._handle.unref();}break;case'FILE':var fs=__webpack_require__(/*! fs */ "fs");stream=new fs.SyncWriteStream(fd,{autoClose:false});stream._type='fs';break;case'PIPE':case'TCP':var net=__webpack_require__(/*! net */ "net");stream=new net.Socket({fd:fd,readable:false,writable:true});stream.readable=false;stream.read=null;stream._type='pipe';if(stream._handle&&stream._handle.unref){stream._handle.unref();}break;default:throw new Error('Implement me. Unknown stream file type!');}stream.fd=fd;stream._isStdio=true;return stream;}function init(debug){debug.inspectOpts={};var keys=Object.keys(exports.inspectOpts);for(var i=0;i<keys.length;i++){debug.inspectOpts[keys[i]]=exports.inspectOpts[keys[i]];}}exports.enable(load());

/***/ }),

/***/ "./node_modules/electron-squirrel-startup/node_modules/ms/index.js":
/*!*************************************************************************!*\
  !*** ./node_modules/electron-squirrel-startup/node_modules/ms/index.js ***!
  \*************************************************************************/
/***/ ((module) => {

var s=1000;var m=s*60;var h=m*60;var d=h*24;var y=d*365.25;module.exports=function(val,options){options=options||{};var type=typeof val;if(type==='string'&&val.length>0){return parse(val);}else if(type==='number'&&isNaN(val)===false){return options.long?fmtLong(val):fmtShort(val);}throw new Error('val is not a non-empty string or a valid number. val='+JSON.stringify(val));};function parse(str){str=String(str);if(str.length>100){return;}var match=/^((?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|years?|yrs?|y)?$/i.exec(str);if(!match){return;}var n=parseFloat(match[1]);var type=(match[2]||'ms').toLowerCase();switch(type){case'years':case'year':case'yrs':case'yr':case'y':return n*y;case'days':case'day':case'd':return n*d;case'hours':case'hour':case'hrs':case'hr':case'h':return n*h;case'minutes':case'minute':case'mins':case'min':case'm':return n*m;case'seconds':case'second':case'secs':case'sec':case's':return n*s;case'milliseconds':case'millisecond':case'msecs':case'msec':case'ms':return n;default:return undefined;}}function fmtShort(ms){if(ms>=d){return Math.round(ms/d)+'d';}if(ms>=h){return Math.round(ms/h)+'h';}if(ms>=m){return Math.round(ms/m)+'m';}if(ms>=s){return Math.round(ms/s)+'s';}return ms+'ms';}function fmtLong(ms){return plural(ms,d,'day')||plural(ms,h,'hour')||plural(ms,m,'minute')||plural(ms,s,'second')||ms+' ms';}function plural(ms,n,name){if(ms<n){return;}if(ms<n*1.5){return Math.floor(ms/n)+' '+name;}return Math.ceil(ms/n)+' '+name+'s';}

/***/ }),

/***/ "child_process":
/*!********************************!*\
  !*** external "child_process" ***!
  \********************************/
/***/ ((module) => {

"use strict";
module.exports = require("child_process");

/***/ }),

/***/ "electron":
/*!***************************!*\
  !*** external "electron" ***!
  \***************************/
/***/ ((module) => {

"use strict";
module.exports = require("electron");

/***/ }),

/***/ "fs":
/*!*********************!*\
  !*** external "fs" ***!
  \*********************/
/***/ ((module) => {

"use strict";
module.exports = require("fs");

/***/ }),

/***/ "net":
/*!**********************!*\
  !*** external "net" ***!
  \**********************/
/***/ ((module) => {

"use strict";
module.exports = require("net");

/***/ }),

/***/ "path":
/*!***********************!*\
  !*** external "path" ***!
  \***********************/
/***/ ((module) => {

"use strict";
module.exports = require("path");

/***/ }),

/***/ "tty":
/*!**********************!*\
  !*** external "tty" ***!
  \**********************/
/***/ ((module) => {

"use strict";
module.exports = require("tty");

/***/ }),

/***/ "util":
/*!***********************!*\
  !*** external "util" ***!
  \***********************/
/***/ ((module) => {

"use strict";
module.exports = require("util");

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/asset-relocator-loader */
/******/ 	if (typeof __webpack_require__ !== 'undefined') __webpack_require__.ab = __dirname + "/native_modules/";
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry needs to be wrapped in an IIFE because it needs to be isolated against other modules in the chunk.
(() => {
/*!**************************!*\
  !*** ./electron/main.ts ***!
  \**************************/
var _electron=__webpack_require__(/*! electron */ "electron");if(__webpack_require__(/*! electron-squirrel-startup */ "./node_modules/electron-squirrel-startup/index.js")){_electron.app.quit();}var createWindow=function createWindow(){var mainWindow=new _electron.BrowserWindow({height:600,width:800,webPreferences:{preload:'/Users/colincarter/Projects/react-native-sqlcipher-storage/example/.webpack/renderer/main_window/preload.js',nodeIntegration:false,contextIsolation:true}});mainWindow.loadURL('http://localhost:3000/main_window/index.html');mainWindow.webContents.openDevTools();};_electron.app.on("ready",createWindow);_electron.app.on("window-all-closed",function(){if(process.platform!=="darwin"){_electron.app.quit();}});_electron.app.on("activate",function(){if(_electron.BrowserWindow.getAllWindows().length===0){createWindow();}});
})();

module.exports = __webpack_exports__;
/******/ })()
;
//# sourceMappingURL=index.js.map