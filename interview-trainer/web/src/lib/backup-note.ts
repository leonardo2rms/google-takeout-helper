import { useSyncExternalStore } from 'react';
import { BACKUP_KEY, backupFileName, dayKey, noteBackupMade, noteBackupSnoozed, readBackupNote, serializeBackupNote, type BackupNote } from '@trainer/shared';
import { copyText, saveFile, type SaveResult } from './files';
import { exportText } from './store';

/**
 * The note about this device's last backup (see shared/src/backup-nudge.ts), in its own storage key. It is a convenience: if it
 * cannot be read or written the only cost is a reminder that comes back, so nothing here raises the data-safety banners.
 */

let memory: string | null = null;
let useMemory = false;

function readRaw(): string | null {
  if (useMemory) return memory;
  try {
    return window.localStorage.getItem(BACKUP_KEY);
  } catch {
    useMemory = true;
    return memory;
  }
}

function writeRaw(text: string): void {
  if (!useMemory) {
    try {
      window.localStorage.setItem(BACKUP_KEY, text);
      return;
    } catch {
      useMemory = true;
    }
  }
  memory = text;
}

const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

let cachedRaw: string | null | undefined;
let cached: BackupNote = readBackupNote(null);

export function loadBackupNote(): BackupNote {
  const raw = readRaw();
  if (raw !== cachedRaw) {
    cachedRaw = raw;
    cached = readBackupNote(raw);
  }
  return cached;
}

function save(n: BackupNote): void {
  writeRaw(serializeBackupNote(n));
  emit();
}

/** Remembers that a backup left this device today. */
export const recordBackup = (): void => save(noteBackupMade(dayKey(new Date())));
/** "Later": no reminder for a week. */
export const snoozeBackupNudge = (): void => save(noteBackupSnoozed(loadBackupNote(), dayKey(new Date())));

/** Saves the backup file and, only if it really was saved, remembers it. */
export async function downloadBackup(): Promise<SaveResult> {
  const result = await saveFile(backupFileName(dayKey(new Date())), exportText(), 'application/json');
  if (result === 'saved') recordBackup();
  return result;
}

/** Copies the backup text and, only if that worked, remembers it. */
export async function copyBackup(): Promise<boolean> {
  const ok = await copyText(exportText());
  if (ok) recordBackup();
  return ok;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  const onStorage = (e: StorageEvent) => {
    if (e.key === BACKUP_KEY || e.key === null) listener();
  };
  window.addEventListener('storage', onStorage);
  return () => {
    listeners.delete(listener);
    window.removeEventListener('storage', onStorage);
  };
}

export const useBackupNote = (): BackupNote => useSyncExternalStore(subscribe, loadBackupNote, loadBackupNote);
