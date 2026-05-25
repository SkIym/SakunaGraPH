import { redirect } from '@sveltejs/kit';

export const POST = async ({ locals: { supabase } }: any) => {
    // console.log("Signin");
    const { data, error } = await supabase.auth.signInWithOAuth({
        provider: 'google',
        options: {
            redirectTo: 'https://sakuna-ph.vercel.app/auth/callback', // Change this when prod: http://localhost:5173/auth/callback / https://sakuna-ph.vercel.app/auth/callback
            queryParams: {
                access_type: 'offline',
                prompt: 'consent',
            },
        },
    });

    if (error) {
        throw redirect(303, '/login/error?code=oauth_failed');
    }

    if (data.url) {
        throw redirect(303, data.url);
    }

    throw redirect(303, '/login/error?code=oauth_failed');
};

