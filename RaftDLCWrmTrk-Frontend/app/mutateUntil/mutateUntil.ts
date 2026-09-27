import { mutate } from 'swr';

type MutateUntilOptions<T> = {
  maxAttempts?: number;
  delayMs?: number;
  isExpected: (data: T) => boolean;
};

export async function mutateUntil<T>(
  key: string,
  {
    maxAttempts = 10,
    delayMs = 500,
    isExpected,
  }: MutateUntilOptions<T>
): Promise<T | undefined> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const data = await mutate<T>(key);

    if (data != undefined && isExpected(data)) {
      return data;
    }

    if (attempt < maxAttempts - 1) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }

  return undefined;
}
