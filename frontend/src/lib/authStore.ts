import { writable } from 'svelte/store';
import { supabase } from './supabase';

export const session = writable(null);
export const displayName = writable('');
export const displayPicture = writable('');

// initialize
supabase.auth.getSession().then(({ data }) => {
  session.set(data.session);
});

// listen for changes (login/logout)
supabase.auth.onAuthStateChange((_event, newSession) => {
  session.set(newSession);
});

// TODO: add userStore