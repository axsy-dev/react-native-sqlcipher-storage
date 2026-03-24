import type { Configuration } from "webpack";
import path from "path";

import { rules } from "./webpack.rules";
import { plugins } from "./webpack.plugins";

// Filter out node-specific loaders (asset-relocator, node-loader) that inject
// __dirname references — these don't exist in the renderer's browser context.
const rendererRules = rules.filter((rule) => {
  if (rule && typeof rule === "object" && "use" in rule) {
    const use = rule.use;
    if (typeof use === "string") {
      return use !== "node-loader";
    }
    if (use && typeof use === "object" && "loader" in use) {
      return !String(use.loader).includes("asset-relocator");
    }
  }
  return true;
});

rendererRules.push({
  test: /\.css$/,
  use: [{ loader: "style-loader" }, { loader: "css-loader" }],
});

export const rendererConfig: Configuration = {
  module: {
    rules: rendererRules,
  },
  plugins,
  resolve: {
    extensions: [".js", ".ts", ".jsx", ".tsx", ".css"],
    alias: {
      "react-native$": "react-native-web",
      react: path.resolve("./node_modules/react"),
      "react-dom": path.resolve("./node_modules/react-dom"),
    },
  },
};
