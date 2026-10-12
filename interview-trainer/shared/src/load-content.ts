/** Node-only: reads the YAML content directory. Never import this from browser code. */
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parse } from 'yaml';
import { Course, validateCourse } from './schema';

export function contentFiles(dir: string): string[] {
  return readdirSync(dir)
    .filter((f) => /\.ya?ml$/.test(f))
    .sort()
    .map((f) => join(dir, f));
}

/** One YAML file per unit; files are ordered by name (01-kotlin.yaml, 02-...). */
export function loadCourse(dir: string): Course {
  const units = contentFiles(dir).map((file) => {
    try {
      return parse(readFileSync(file, 'utf8'));
    } catch (e) {
      throw new Error(`${file}: ${(e as Error).message}`);
    }
  });
  const parsed = Course.safeParse({ units });
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
    throw new Error(`Invalid course content:\n${issues}`);
  }
  const problems = validateCourse(parsed.data);
  if (problems.length) throw new Error(`Invalid course content:\n${problems.map((p) => `  ${p}`).join('\n')}`);
  return parsed.data;
}
