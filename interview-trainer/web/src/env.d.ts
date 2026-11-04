/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "1" in the hosted single-file build: no API server, no service worker, AI via `sample`. */
  readonly VITE_STANDALONE?: string;
  readonly VITE_API_URL?: string;
}

declare module 'virtual:course' {
  import type { Course } from '@trainer/shared';
  const course: Course;
  export default course;
}
