import type IForkTsCheckerWebpackPlugin from "fork-ts-checker-webpack-plugin";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const ForkTsCheckerWebpackPlugin: typeof IForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin");

// eslint-disable-next-line @typescript-eslint/no-var-requires
const webpack = require("webpack");

export const plugins = [
  new ForkTsCheckerWebpackPlugin({
    logger: "webpack-infrastructure",
    typescript: {
      configFile: "./electron/tsconfig.json"
    }
  }),
  new webpack.DefinePlugin({
    __DEV__: process.env.NODE_ENV === "developement"
  })
];
