import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { deleteApp, initializeApp } from 'firebase/app';
import { getAuth, signInAnonymously } from 'firebase/auth';
import { collection, getDocs, getFirestore } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: 'AIzaSyBXgjqxXu1icjHxrIhal1ncQ6ZwqDr5E64',
  authDomain: 'xiaoshouyejifenxi.firebaseapp.com',
  projectId: 'xiaoshouyejifenxi',
  storageBucket: 'xiaoshouyejifenxi.firebasestorage.app',
  messagingSenderId: '951263111259',
  appId: '1:951263111259:web:0877b8556416dbb90ff77e',
  measurementId: 'G-YSQJYXYSNX',
};

const collections = ['contracts', 'payments', 'meta'];

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const backupRoot = process.env.BACKUP_ROOT || path.join(repoRoot, 'backups', 'firestore');
const nutstoreParent = '/Users/wang/Nutstore Files/.symlinks/坚果云';
const nutstoreBackupRoot =
  process.env.BACKUP_NUTSTORE_ROOT || path.join(nutstoreParent, 'yejifenxi-backups', 'firestore');
const requiredNonEmptyCollections = ['contracts', 'payments'];

const pad = (n) => String(n).padStart(2, '0');

const toIso = (value) => {
  if (value instanceof Date) return value.toISOString();
  if (value && typeof value.toDate === 'function') return value.toDate().toISOString();
  return value;
};

const serialize = (value) => {
  if (value === null || value === undefined) return value;
  const asIso = toIso(value);
  if (asIso !== value) return asIso;
  if (Array.isArray(value)) return value.map((item) => serialize(item));
  if (typeof value === 'object') {
    const obj = {};
    for (const [k, v] of Object.entries(value)) {
      obj[k] = serialize(v);
    }
    return obj;
  }
  return value;
};

const sortRows = (rows) =>
  rows
    .slice()
    .sort((a, b) => String(a.id || '').localeCompare(String(b.id || ''), 'zh-CN', { numeric: true }));

const pruneOldDailyDirs = async (rootDir, keepDays) => {
  try {
    const entries = await fs.readdir(rootDir, { withFileTypes: true });
    const now = Date.now();
    await Promise.all(
      entries
        .filter((entry) => entry.isDirectory() && /^20\d{2}-\d{2}-\d{2}$/.test(entry.name))
        .map(async (entry) => {
          const fullPath = path.join(rootDir, entry.name);
          const stat = await fs.stat(fullPath);
          const ageMs = now - stat.mtimeMs;
          if (ageMs > keepDays * 24 * 60 * 60 * 1000) {
            await fs.rm(fullPath, { recursive: true, force: true });
          }
        })
    );
  } catch {
    // Ignore prune failures to avoid blocking backup success.
  }
};

const readLatestManifest = async (rootDir) => {
  try {
    const raw = await fs.readFile(path.join(rootDir, 'latest.json'), 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
};

const assertHealthySnapshot = (manifest, previousManifest) => {
  const counts = manifest.collections || {};
  const emptyRequired = requiredNonEmptyCollections.filter((name) => (counts[name] || 0) === 0);
  if (emptyRequired.length) {
    throw new Error(`backup sanity check failed: empty required collections -> ${emptyRequired.join(', ')}`);
  }

  if (Object.values(counts).every((count) => count === 0)) {
    throw new Error('backup sanity check failed: all collections are empty');
  }

  if (!previousManifest?.collections) return;

  for (const name of requiredNonEmptyCollections) {
    const prev = Number(previousManifest.collections[name] || 0);
    const next = Number(counts[name] || 0);
    if (prev > 0 && next === 0) {
      throw new Error(`backup sanity check failed: ${name} dropped from ${prev} to 0`);
    }
  }
};

let lastTargetDir = '';

const run = async () => {
  const now = new Date();
  const dayDir = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
  const timeDir = `${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
  const targetDir = path.join(backupRoot, dayDir, timeDir);
  lastTargetDir = targetDir;
  await fs.mkdir(targetDir, { recursive: true });

  const app = initializeApp(firebaseConfig);
  const auth = getAuth(app);
  await signInAnonymously(auth);
  const db = getFirestore(app);

  const manifest = {
    generatedAt: now.toISOString(),
    projectId: firebaseConfig.projectId,
    collections: {},
  };
  const snapshotData = {};

  for (const name of collections) {
    const snap = await getDocs(collection(db, name));
    const rows = sortRows(
      snap.docs.map((docSnap) => ({
        id: docSnap.id,
        ...serialize(docSnap.data()),
      }))
    );
    snapshotData[name] = rows;
    manifest.collections[name] = rows.length;
  }

  const previousManifest = await readLatestManifest(backupRoot);
  assertHealthySnapshot(manifest, previousManifest);

  for (const name of collections) {
    await fs.writeFile(
      path.join(targetDir, `${name}.json`),
      `${JSON.stringify(snapshotData[name], null, 2)}\n`,
      'utf8'
    );
  }

  await fs.writeFile(path.join(targetDir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  await fs.writeFile(path.join(backupRoot, 'latest.json'), `${JSON.stringify({ ...manifest, path: targetDir }, null, 2)}\n`, 'utf8');

  // Optional secondary mirror to Nutstore path when available.
  try {
    await fs.access(nutstoreParent);
    const relativePath = path.relative(backupRoot, targetDir);
    const mirrorDir = path.join(nutstoreBackupRoot, relativePath);
    await fs.mkdir(path.dirname(mirrorDir), { recursive: true });
    await fs.cp(targetDir, mirrorDir, { recursive: true, force: true });
    await fs.writeFile(
      path.join(nutstoreBackupRoot, 'latest.json'),
      `${JSON.stringify({ ...manifest, path: mirrorDir }, null, 2)}\n`,
      'utf8'
    );
  } catch {
    // Skip mirror when Nutstore path is not available.
  }

  await pruneOldDailyDirs(backupRoot, 90);
  await pruneOldDailyDirs(nutstoreBackupRoot, 90);

  process.stdout.write(
    `backup ok: ${targetDir}\ncounts: ${JSON.stringify(manifest.collections)}\n`
  );
  await auth.signOut().catch(() => {});
  await deleteApp(app).catch(() => {});
};

run()
  .then(() => {
    process.exit(0);
  })
  .catch(async (err) => {
    try {
      if (lastTargetDir) {
        await fs.rm(lastTargetDir, { recursive: true, force: true });
      }
    } catch {
      // Ignore cleanup failure after backup failure.
    }
    process.stderr.write(`backup failed: ${err?.stack || err}\n`);
    process.exit(1);
  });
