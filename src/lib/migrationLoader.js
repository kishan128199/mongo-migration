const fs = require('fs').promises;
const path = require('path');

function normaliseName(fileName) {
  return fileName.replace(/\.js$/, '');
}

async function listMigrations(dir) {
  let entries;
  try {
    entries = await fs.readdir(dir);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }

  return entries
    .filter((file) => file.endsWith('.js'))
    .map((file) => ({
      id: normaliseName(file),
      fileName: file,
      fullPath: path.join(dir, file),
    }))
    .sort((a, b) => a.fileName.localeCompare(b.fileName));
}

async function loadMigration(dir, name) {
  const migrations = await listMigrations(dir);
  const normalised = normaliseName(name);
  const match = migrations.find((migration) => migration.id === normalised);

  if (!match) {
    throw new Error(`Migration '${name}' not found in ${dir}`);
  }

  delete require.cache[require.resolve(match.fullPath)];
  const moduleExports = require(match.fullPath);

  if (typeof moduleExports.up !== 'function') {
    throw new Error(`Migration '${name}' must export an 'up' function`);
  }

  return {
    id: match.id,
    description: moduleExports.description || '',
    up: moduleExports.up,
  };
}

module.exports = {
  listMigrations,
  loadMigration,
};
