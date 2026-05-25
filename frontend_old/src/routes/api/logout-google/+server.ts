import { redirect } from '@sveltejs/kit';

export const POST = async ({ locals: { supabase } }: any)  => {
  const { error } = await supabase.auth.signOut()

  if (error) {
    console.error('Logout error:', error.message)
    return
  }
  throw redirect(302, '/');
};
