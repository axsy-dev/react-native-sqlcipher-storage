import type { Configuration } from "webpack";

import { rules } from "./webpack.rules";
import { plugins } from "./webpack.plugins";

rules.push({
  // We're specifying native_modules in the test because the asset relocator loader generates a
  // "fake" .node file which is really a cjs file.
  test: /native_modules[/\\].+\.node$/,
  use: "node-loader"
});

rules.push({
  test: /[/\\]node_modules[/\\].+\.(m?js|node)$/,
  parser: { amd: false },
  use: {
    loader: "@vercel/webpack-asset-relocator-loader",
    options: {
      outputAssetBase: "native_modules"
    }
  }
});

export const mainConfig: Configuration = {
  /**
   * This is the main entry point for your application, it's the first file
   * that runs in the main process.
   */
  entry: "./electron/main.ts",
  // Put your normal webpack config below here
  module: {
    rules
  },
  plugins,
  resolve: {
    extensions: [".js", ".ts", ".jsx", ".tsx", ".css", ".json"]
  }
};
