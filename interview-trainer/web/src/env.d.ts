/// <reference types="vite/client" />

declare module 'virtual:course' {
  import type { Course } from '@trainer/shared';
  const course: Course;
  export default course;
}
