import { initializeApp } from 'firebase/app';
import { getDatabase } from 'firebase/database';

// Cấu hình từ Firebase Console (Web app). Realtime Database URL thường có dạng:
// https://<project-id>-default-rtdb.firebaseio.com
// hoặc https://<project-id>-default-rtdb.<region>.firebasedatabase.app
const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY || "YOUR_API_KEY",
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN || "YOUR_PROJECT_ID.firebaseapp.com",
  databaseURL: import.meta.env.VITE_FIREBASE_DATABASE_URL || "https://YOUR_PROJECT_ID.firebaseio.com",
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID || "YOUR_PROJECT_ID",
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET || "YOUR_PROJECT_ID.appspot.com",
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID || "YOUR_SENDER_ID",
  appId: import.meta.env.VITE_FIREBASE_APP_ID || "YOUR_APP_ID"
};

export const firebaseConfigured =
  Boolean(
    import.meta.env.VITE_FIREBASE_DATABASE_URL &&
      !String(import.meta.env.VITE_FIREBASE_DATABASE_URL).includes("YOUR_PROJECT_ID")
  );

// Khởi tạo Firebase
const app = initializeApp(firebaseConfig);

// Khởi tạo Realtime Database
export const db = getDatabase(app);
