// D3I fork lint policy, composed onto the upstream config by eslint.config.js
// (whose divergence from upstream eyra/feldspar is exactly two lines: the
// import of this module and its spread at the array tail). Flat config: these
// objects come after upstream's, and the last matching object wins.
import globals from 'globals'

export default [
  {
    // Core no-unused-vars misfires on TypeScript type syntax: it flags the
    // required parameter names in function-type annotations. Documented remedy
    // is disabling the core rule in favor of the extension rule, which
    // upstream already enables ('warn'); the lint script's --max-warnings 0
    // gates warnings. typescript-eslint FAQ: "Why is a rule from ESLint core
    // not working correctly with TypeScript code?"
    files: ['**/*.{ts,tsx}'],
    rules: {
      'no-unused-vars': 'off',
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
  {
    // Jest globals for the fork's test suite (upstream data-collector has no
    // tests, so its config never needed these).
    files: ['src/**/*.test.{ts,tsx}'],
    languageOptions: {
      globals: globals.jest,
    },
  },
]
