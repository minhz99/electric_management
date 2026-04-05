import { initializeApp, type FirebaseOptions } from 'firebase/app';
import { getDatabase } from 'firebase/database';

/**
 * Chỉ chấp nhận URL Realtime Database thật (HTTPS + host Firebase).
 * Trùng với FIREBASE_DATABASE_URL phía Python / export JSON.
 */
function resolveDatabaseUrl(): string | null {
  const raw = String(import.meta.env.VITE_FIREBASE_DATABASE_URL ?? '').trim();
  if (!raw || raw.includes('YOUR_PROJECT')) return null;
  try {
    const u = new URL(raw);
    if (u.protocol !== 'https:') return null;
    const h = u.hostname;
    if (!h.endsWith('firebaseio.com') && !h.endsWith('firebasedatabase.app')) return null;
    return raw.replace(/\/$/, '');
  } catch {
    return null;
  }
}

export const firebaseDatabaseUrl = resolveDatabaseUrl();
export const firebaseConfigured = Boolean(firebaseDatabaseUrl);

/** Host hiển thị trên UI (không chứa API key). */
export function getDatabaseHostLabel(): string {
  if (!firebaseDatabaseUrl) return '—';
  try {
    return new URL(firebaseDatabaseUrl).hostname;
  } catch {
    return '—';
  }
}

const placeholderConfig: FirebaseOptions = {
  apiKey: 'unused',
  authDomain: 'unused.firebaseapp.com',
  databaseURL: 'https://unused-placeholder.firebaseio.com',
  projectId: 'unused',
  storageBucket: 'unused.appspot.com',
  messagingSenderId: '0',
  appId: '1:0:web:0',
};

const appConfig: FirebaseOptions = firebaseDatabaseUrl
  ? {
      apiKey: import.meta.env.VITE_FIREBASE_API_KEY || '',
      authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN || '',
      databaseURL: firebaseDatabaseUrl,
      projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID || '',
      storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET || '',
      messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID || '',
      appId: import.meta.env.VITE_FIREBASE_APP_ID || '',
    }
  : placeholderConfig;

const app = initializeApp(appConfig);
export const db = getDatabase(app);
