#!/usr/bin/env node

const shell = require('shelljs');
const archiver = require('archiver');
const fs = require('fs');
const path = require('path');

// Set environment
process.env.NODE_ENV = 'production';

// Get project name from current directory
const name = path.basename(process.cwd());

// Create releases directory if it doesn't exist
shell.mkdir('-p', 'releases');

// Count existing releases and increment
const releaseFiles = shell.ls('releases/*').filter(file => !file.includes('releases/')).length || 0;
const nr = releaseFiles + 1;

// Get timestamp
const timestamp = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format

console.log('Building project...');
if (shell.exec('pnpm run build').code !== 0) {
  shell.echo('Error: Build failed');
  shell.exit(1);
}

// Create zip file
const distPath = path.join('packages', 'data-collector', 'dist');
const zipFileName = `${name}_${timestamp}_${nr}.zip`;
const zipPath = path.join('releases', zipFileName);

console.log(`Creating release: ${zipFileName}`);

const output = fs.createWriteStream(zipPath);
const archive = archiver('zip', {
  zlib: { level: 9 } // Maximum compression
});

output.on('close', () => {
  console.log(`Release created successfully: ${zipPath}`);
  console.log(`Total bytes: ${archive.pointer()}`);
});

archive.on('error', (err) => {
  throw err;
});

archive.pipe(output);
archive.directory(distPath, false);
archive.finalize();