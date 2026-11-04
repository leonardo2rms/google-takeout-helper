import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadCourse } from '../shared/src/load-content';

const dir = resolve(dirname(fileURLToPath(import.meta.url)), '../content');
try {
  const course = loadCourse(dir);
  const counts: Record<string, number> = {};
  for (const u of course.units) {
    let n = 0;
    for (const l of u.lessons) {
      for (const q of l.questions) {
        counts[q.type] = (counts[q.type] ?? 0) + 1;
        n++;
      }
      console.log(`  ${u.id}/${l.id}: ${l.questions.length} questions`);
    }
    console.log(`${u.emoji} ${u.title}: ${u.lessons.length} lessons, ${n} questions`);
  }
  console.log('\nBy type:', counts);
  console.log('Content OK');
} catch (e) {
  console.error((e as Error).message);
  process.exit(1);
}
