import { initializeApp, deleteApp } from 'firebase/app';
import { getAuth, signInAnonymously } from 'firebase/auth';
import {
  collection,
  doc,
  getDocs,
  getFirestore,
  serverTimestamp,
  writeBatch,
} from 'firebase/firestore';

const firebaseConfig = {
  apiKey: 'AIzaSyBXgjqxXu1icjHxrIhal1ncQ6ZwqDr5E64',
  authDomain: 'xiaoshouyejifenxi.firebaseapp.com',
  projectId: 'xiaoshouyejifenxi',
  storageBucket: 'xiaoshouyejifenxi.firebasestorage.app',
  messagingSenderId: '951263111259',
  appId: '1:951263111259:web:0877b8556416dbb90ff77e',
  measurementId: 'G-YSQJYXYSNX',
};

const contractTypes = new Set(['SAAS', '私有部署订阅', '私有部署买断']);
const paymentTypes = new Set(['新签', '增购', '续约', '维保', '升级费']);
const customerTypes = new Set(['新客户', '老客户']);
const indicatorTypes = new Set(['新客户指标', '老客户指标']);
const quarters = new Set(['Q1', 'Q2', 'Q3', 'Q4']);

const parseNumber = (value) => {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value === 'string') {
    const normalized = value.replace(/[￥¥,\s]/g, '').trim();
    if (!normalized) return 0;
    const num = Number(normalized);
    return Number.isFinite(num) ? num : 0;
  }
  return 0;
};

const hasText = (value) => String(value ?? '').trim() !== '';

const isInvalidContractSeed = (row) =>
  row.source === 'seed' &&
  (!hasText(row.customer) ||
    !hasText(row.sales) ||
    !contractTypes.has(String(row.type || '').trim()) ||
    !quarters.has(String(row.quarter || '').trim()) ||
    parseNumber(row.amount) <= 0);

const isInvalidPaymentSeed = (row) => {
  const amount = parseNumber(row.amount);
  const totalCost = parseNumber(row.totalCost);
  const actualAccrual = parseNumber(row.actualAccrual);
  return (
    row.source === 'seed' &&
    (!hasText(row.customer) ||
      !hasText(row.sales) ||
      !customerTypes.has(String(row.customerType || '').trim()) ||
      !paymentTypes.has(String(row.contractType || '').trim()) ||
      !indicatorTypes.has(String(row.indicator || '').trim()) ||
      !quarters.has(String(row.quarter || '').trim()) ||
      ![amount, totalCost, actualAccrual].some((value) => Math.abs(value) > 0))
  );
};

const softDelete = async (db, collectionName, ids) => {
  const chunkSize = 400;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const batch = writeBatch(db);
    ids.slice(i, i + chunkSize).forEach((id) => {
      batch.set(
        doc(db, collectionName, id),
        {
          deletedAt: serverTimestamp(),
          deletedReason: 'invalid-seed-cleanup',
          deletedBy: 'codex',
        },
        { merge: true }
      );
    });
    await batch.commit();
  }
};

const run = async () => {
  const app = initializeApp(firebaseConfig);
  const auth = getAuth(app);
  await signInAnonymously(auth);
  const db = getFirestore(app);

  const [contractSnap, paymentSnap] = await Promise.all([
    getDocs(collection(db, 'contracts')),
    getDocs(collection(db, 'payments')),
  ]);

  const contracts = contractSnap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
  const payments = paymentSnap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));

  const invalidContracts = contracts.filter((row) => !row.deletedAt && isInvalidContractSeed(row));
  const invalidPayments = payments.filter((row) => !row.deletedAt && isInvalidPaymentSeed(row));

  await softDelete(
    db,
    'contracts',
    invalidContracts.map((row) => row.id)
  );
  await softDelete(
    db,
    'payments',
    invalidPayments.map((row) => row.id)
  );

  console.log(
    JSON.stringify(
      {
        invalidContractCount: invalidContracts.length,
        invalidPaymentCount: invalidPayments.length,
        invalidContractSample: invalidContracts.slice(0, 5).map(({ id, customer, sales, type, amount }) => ({
          id,
          customer,
          sales,
          type,
          amount,
        })),
        invalidPaymentSample: invalidPayments.slice(0, 5).map(
          ({ id, customer, sales, contractType, indicator, quarter, amount }) => ({
            id,
            customer,
            sales,
            contractType,
            indicator,
            quarter,
            amount,
          })
        ),
      },
      null,
      2
    )
  );

  await auth.signOut().catch(() => {});
  await deleteApp(app).catch(() => {});
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
