import { supabase } from '$lib/supabase';
// import { session } from '$lib/authStore';
export const load = async ({ data, depends }: any) => {
  depends('supabase:auth');

  const { data: { session } } = await supabase.auth.getSession();

  return {
    ...data,
    supabase,
    session,
  };
};