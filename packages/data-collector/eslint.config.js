import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from '@typescript-eslint/eslint-plugin'
import tsParser from '@typescript-eslint/parser'

export default [
  { ignores: ['dist'] },
  {
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      globals: globals.browser,
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        ecmaFeatures: {
          jsx: true,
        },
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...js.configs.recommended.rules,
      '@typescript-eslint/no-unused-vars': 'warn',
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': [
        'warn',
        { allowConstantExport: true },
      ],
    },
  },
  {
    // ADR-0035: per-row loops in the viz data pipeline must not construct
    // ICU machinery per call — hoist Intl formatters (see util.ts formatDate).
    files: ['src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/**/*.{ts,tsx}'],
    ignores: [
      'src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/util.ts',
      'src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/**/*.test.ts',
    ],
    rules: {
      'no-restricted-syntax': [
        'error',
        {
          selector: "CallExpression[callee.property.name=/^toLocale(Date|Time)?String$/]",
          message: 'Constructs ICU machinery per call; hoist an Intl.DateTimeFormat instead (ADR-0035, see util.ts formatDate).',
        },
        {
          selector: "NewExpression[callee.object.name='Intl']",
          message: 'Construct Intl formatters once in util.ts and reuse (ADR-0035).',
        },
      ],
    },
  },
]
