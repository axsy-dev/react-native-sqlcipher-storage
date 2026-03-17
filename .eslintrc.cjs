module.exports = {
  root: true,
  extends: ['@react-native', 'plugin:prettier/recommended'],
  overrides: [
    {
      files: ['*.js'],
      parser: '@babel/eslint-parser',
      parserOptions: {
        requireConfigFile: false,
        sourceType: 'module',
        babelOptions: {
          configFile: false,
          presets: ['@react-native/babel-preset'],
        },
      },
    },
    {
      files: ['electron/**/*.js'],
      env: {
        node: true,
      },
    },
  ],
};
