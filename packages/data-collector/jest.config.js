/** @type {import('ts-jest').JestConfigWithTsJest} */
export default {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/*.test.ts'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx'],
  // The built @eyra/feldspar cannot be resolved from jest (its exports map has
  // no "require" condition). Map the specifier onto a source re-export so tests
  // exercise the real feldspar code. See src/test_support/feldspar_test_shim.ts.
  moduleNameMapper: {
    '^@eyra/feldspar$': '<rootDir>/src/test_support/feldspar_test_shim.ts',
  },
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      useESM: true,
      tsconfig: '<rootDir>/tsconfig.test.json',
    }],
  },
  extensionsToTreatAsEsm: ['.ts', '.tsx'],
};
