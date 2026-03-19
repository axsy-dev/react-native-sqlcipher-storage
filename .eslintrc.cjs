module.exports = {
  root: true,
  ignorePatterns: ["src/index.js", "src/sqlite.core.js"],
  extends: ["@react-native", "plugin:prettier/recommended"],
  rules: {
    "no-shadow": "off",
    "no-new": "off",
    "no-unused-vars": "off",
  },
  overrides: [
    {
      files: ["*.js"],
      parser: "@babel/eslint-parser",
      parserOptions: {
        requireConfigFile: false,
        sourceType: "module",
        babelOptions: {
          configFile: false,
          presets: ["@react-native/babel-preset"],
        },
      },
    },
    {
      files: ["electron/**/*.js"],
      env: {
        node: true,
      },
    },
  ],
};
