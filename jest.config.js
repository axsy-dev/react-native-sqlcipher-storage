export default {
  transform: {
    "\\.[jt]sx?$": [
      "babel-jest",
      {presets: ["module:@react-native/babel-preset"]},
    ],
  },
  transformIgnorePatterns: ["node_modules/(?!(react-native|@react-native)/)"],
  testEnvironment: "node",
  testMatch: ["**/__tests__/**/*.test.js"],
};
