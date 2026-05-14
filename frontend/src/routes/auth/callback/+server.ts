import { redirect } from '@sveltejs/kit';

export const GET = async (event) => {
	const {
		url,
		locals: { supabase },
	} = event;

  console.log('callback hit');
  console.log('full URL:', url.toString());
  console.log('code:', url.searchParams.get('code'));

	const code = url.searchParams.get('code') as string;
	const next = url.searchParams.get('next') ?? '/';

  if (code) {
    const { error } = await supabase.auth.exchangeCodeForSession(code)
    if (!error) {
      const { data: { user }, error: userError } = await supabase.auth.getUser();
      // console.log('user after exchange:', user); 

      if (user && !userError) {
        const { data: profile, error: profileError } = await supabase
          .from('profiles')
          .select('display_name, birthdate, occupation, intended_usage')
          .eq('id', user.id)
          .order('updated_at', { ascending: false })
          .limit(1)
          .maybeSingle();

        // console.log('profile:', profile);          // check this
        // console.log('profile error:', profileError); // check this

        const isIncomplete =
          !profile ||
          !profile.display_name ||
          !profile.birthdate ||
          !profile.occupation ||
          !profile.intended_usage?.length;
        console.log("Is incomplete: ",isIncomplete);
        if (isIncomplete) {
          console.log('Profile incomplete, redirecting to setup');
          throw redirect(303, '/?setup=true');
        }
      }
      throw redirect(303, `/${next.slice(1)}`);
    }
  }

  // return the user to an error page with instructions
  throw redirect(303, '/auth/auth-code-error');
};
