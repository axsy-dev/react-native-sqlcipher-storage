import webpack from "webpack";
import type { Configuration } from "webpack";

import { plugins } from "./webpack.plugins";

export const preloadConfig: Configuration = {
  target: "electron-preload",
  devtool: "inline-source-map",
  module: {
    rules: [
      {
        test: /\.(js|mjs|jsx|ts|tsx)$/,
        exclude: [
          /node_modules/,
          /\.(test|spec)\.(ts|tsx|js|jsx)$/i,
        ],
        use: {
          loader: "babel-loader",
          options: {
            presets: [
              [
                "module:@react-native/babel-preset",
                { disableImportExportTransform: true },
              ],
            ],
            plugins: [
              [
                "@babel/plugin-transform-flow-strip-types",
                { allowDeclareFields: true },
              ],
            ],
            overrides: [
              {
                test: ["**/*.ts", "**/*.tsx"],
                plugins: [
                  [
                    "@babel/plugin-transform-typescript",
                    { onlyRemoveTypeImports: true, allowDeclareFields: true },
                  ],
                ],
              },
            ],
          },
        },
      },
    ],
  },
  plugins: [
    ...plugins,
    new webpack.DefinePlugin({
      __dirname: JSON.stringify(""),
    }),
  ],
  resolve: {
    extensions: [".js", ".ts", ".jsx", ".tsx", ".css", ".json"],
  },
};
