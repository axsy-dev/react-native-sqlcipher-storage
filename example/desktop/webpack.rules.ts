import type { ModuleOptions } from "webpack";

export const rules: Required<ModuleOptions>["rules"] = [
  // Add support for native node modules
  {
    // We're specifying native_modules in the test because the asset relocator loader generates a
    // "fake" .node file which is really a cjs file.
    test: /native_modules[/\\].+\.node$/,
    use: "node-loader",
  },
  {
    test: /[/\\]node_modules[/\\].+\.(m?js|node)$/,
    parser: { amd: false },
    use: {
      loader: "@vercel/webpack-asset-relocator-loader",
      options: {
        outputAssetBase: "native_modules",
      },
    },
  },
  {
    test: /\.(js|mjs|jsx|ts|tsx)$/,
    exclude: [
      /node_modules(?![/\\]react-native|[/\\]react-native-sqlcipher-storage|[/\\]@react-native)/,
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
];
