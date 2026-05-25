import type { LayoutLoad } from './$types';

// ssr: false so this only runs on the client, keeping pages responsive immediately
export const ssr = false;

export const load: LayoutLoad = async ({ fetch }) => {
    const res = await fetch('/api/events');
    const events = await res.json();
    return { events };
};
