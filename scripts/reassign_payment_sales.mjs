import { initializeApp, deleteApp } from 'firebase/app';
import { getAuth, signInAnonymously } from 'firebase/auth';
import { collection, getDocs, getFirestore, doc, setDoc, writeBatch } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: 'AIzaSyBXgjqxXu1icjHxrIhal1ncQ6ZwqDr5E64',
  authDomain: 'xiaoshouyejifenxi.firebaseapp.com',
  projectId: 'xiaoshouyejifenxi',
  storageBucket: 'xiaoshouyejifenxi.firebasestorage.app',
  messagingSenderId: '951263111259',
  appId: '1:951263111259:web:0877b8556416dbb90ff77e',
  measurementId: 'G-YSQJYXYSNX',
};

const rules = [
  {
    keyword: '上海隐冠半导体技术',
    fromSales: '张柳云',
    toSales: '王雪靖',
  },
  {
    keyword: '江苏菲沃泰纳米科技',
    fromSales: '张柳云',
    toSales: '唐龙军',
  },
];

const run = async () => {
  const app = initializeApp(firebaseConfig);
  const auth = getAuth(app);
  await signInAnonymously(auth);
  const db = getFirestore(app);

  const snap = await getDocs(collection(db, 'payments'));
  const rows = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
  const matched = [];

  rules.forEach((rule) => {
    rows.forEach((row) => {
      const customer = String(row.customer || '');
      const sales = String(row.sales || '');
      if (sales !== rule.fromSales) return;
      if (!customer.includes(rule.keyword)) return;
      matched.push({
        id: row.id,
        customer,
        fromSales: sales,
        toSales: rule.toSales,
      });
    });
  });

  if (!matched.length) {
    console.log('no matched records');
    await auth.signOut().catch(() => {});
    await deleteApp(app).catch(() => {});
    return;
  }

  const uniqueById = new Map();
  matched.forEach((item) => uniqueById.set(item.id, item));
  const updates = Array.from(uniqueById.values());

  const batch = writeBatch(db);
  updates.forEach((item) => {
    const ref = doc(db, 'payments', item.id);
    batch.set(ref, { sales: item.toSales }, { merge: true });
  });
  await batch.commit();

  console.log(`updated ${updates.length} records`);
  updates.forEach((item) => {
    console.log(`${item.id} | ${item.customer} | ${item.fromSales} -> ${item.toSales}`);
  });

  await auth.signOut().catch(() => {});
  await deleteApp(app).catch(() => {});
};

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
