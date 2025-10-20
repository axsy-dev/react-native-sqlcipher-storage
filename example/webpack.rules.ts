import type { ModuleOptions } from "webpack";

export const rules: Required<ModuleOptions>["rules"] = [
  {
    test: /\.(js|mjs|jsx|ts|tsx)$/,
    exclude: /\.(test|spec)\.(ts|tsx|js|jsx)$/i,
    use: {
      loader: "babel-loader",
      options: {
        presets: ["module:@react-native/babel-preset"],
        plugins: [
          [
            "@babel/plugin-transform-flow-strip-types",
            { allowDeclareFields: true }
          ]
        ],
        overrides: [
          {
            test: ["**/*.ts", "**/*.tsx"],
            plugins: [
              [
                "@babel/plugin-transform-typescript",
                { onlyRemoveTypeImports: true, allowDeclareFields: true }
              ]
            ]
          }
        ]
      }
    }
  }
];
