import type { LayoutServerLoad } from './$types';
// import { session } from '$lib/authStore';

export const load: LayoutServerLoad = async ({ locals }) => {
  const user = await locals.safeGetUser();
  const { data: { session } } = await locals.supabase.auth.getSession();

  return {
    user,
    session,
  };
};