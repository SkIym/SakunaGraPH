<script>
// @ts-nocheck

	// stores
	import { page } from '$app/state';
	import { session, displayName, displayPicture } from '$lib/authStore';
	import { supabase } from '$lib/supabase';

	// svelte components
	import logo from '$lib/images/philippines_logo.svg';
	import NavBar from '$lib/components/Header/NavBar.svelte';
	import NavItem from '$lib/components/Header/NavItem.svelte';
	import google from '$lib/images/google-color.svg';
	import FormModal from '$lib/components/formModal.svelte';

	import { Button, DarkMode } from "flowbite-svelte";

	let user = $derived(page.data.user);
	let showModal = $state(false);
	let mobileMenuOpen = $state(false);

	function toggleModal() {
		showModal = true;
	}

	function toggleMobileMenu() {
		mobileMenuOpen = !mobileMenuOpen;
	}

	function closeMobileMenu() {
		mobileMenuOpen = false;
	}

	$effect(() => {
		async function fetchProfile() {
			if (!$session?.user?.id) return;
			const { data: profile } = await supabase
				.from('profiles')
				.select('display_name')
				.eq('id', $session.user.id)
				.maybeSingle();

			displayName.set(
				profile?.display_name ||
				$session?.user?.user_metadata?.full_name ||
				''
			);
			displayPicture.set(
				$session?.user?.user_metadata?.avatar_url ||
				$session?.user?.user_metadata?.picture ||
				''
			);
		}

		fetchProfile();
	});
</script>

<header class="fixed z-300 bg-(--color-theme-2) w-full max-w-full">
	<div class="px-4 grid grid-cols-[auto_1fr_auto] items-center max-w-360 mx-auto">

		<!-- Left: Logo (always visible) -->
		<div class="flex h-16 items-center justify-center text-white font-bold">
			<a href="/" class="flex items-center px-4">
				<img src={logo} alt="Home" class="h-17.5 w-17.5 p-2.5" />
				<h3 class="text-base hidden sm:block">Sakuna.PH</h3>
			</a>
		</div>

		<!-- Center: Desktop nav OR mobile hamburger -->
		<nav class="flex h-16 items-center justify-center">
			<!-- Desktop nav (hidden on mobile) -->
			<ul class="hidden md:flex h-16 items-center gap-1">
				<NavBar label="Home" href="/"/>
				<NavBar label="About" href="/about" match="/about">
					<NavItem label="Sakuna PH" href="/about#about"></NavItem>
					<NavItem label="Team" href="/about#team"></NavItem>
				</NavBar>
				<NavBar label="Disasters" href="/disasters" match="/disasters/">
					<NavItem label="Map" href="/disasters/map"></NavItem>
					<NavItem label="Table" href="/disasters/table"></NavItem>
					<NavItem label="Metric" href="/disasters/metric"></NavItem>
					<NavItem label="Timeline" href="/disasters/timeline"></NavItem>
				</NavBar>
			</ul>

			<!-- Mobile hamburger (visible only on small screens) -->
			<button
				class="md:hidden flex items-center justify-center text-white p-2 rounded-md hover:bg-white/10 transition-colors"
				onclick={toggleMobileMenu}
				aria-label="Toggle menu"
				aria-expanded={mobileMenuOpen}
			>
				{#if mobileMenuOpen}
					<!-- X icon -->
					<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
						<line x1="18" y1="6" x2="6" y2="18"></line>
						<line x1="6" y1="6" x2="18" y2="18"></line>
					</svg>
				{:else}
					<!-- Hamburger icon -->
					<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
						<line x1="3" y1="6" x2="21" y2="6"></line>
						<line x1="3" y1="12" x2="21" y2="12"></line>
						<line x1="3" y1="18" x2="21" y2="18"></line>
					</svg>
				{/if}
			</button>
		</nav>

		<!-- Right: Account + Dark Mode -->
		<div class="flex h-16 items-center justify-center text-white font-bold gap-2 md:gap-4">
			{#if user}
				<!-- Desktop: full name + dropdown -->
				<div class="hidden md:flex items-center justify-center text-white font-bold">
					<nav class="flex h-16 items-center justify-center">
						<ul class="flex h-16 items-center gap-1">
							<NavBar label={$displayName} image={$displayPicture} href="#" match="/profile">
								<NavItem>
									<div class="flex flex-row">
										<form action="" method="POST" class="flex flew-row">
											<button type="button" onclick={toggleModal} class="flex items-center w-full">
												<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 16 16" class="pr-1"><path fill="currentColor" fill-rule="evenodd" d="M10.5 5a2.5 2.5 0 1 1-5 0a2.5 2.5 0 0 1 5 0m.514 2.63a4 4 0 1 0-6.028 0A4 4 0 0 0 2 11.5V13a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-1.5a4 4 0 0 0-2.986-3.87M8 9H6a2.5 2.5 0 0 0-2.5 2.5V13a.5.5 0 0 0 .5.5h8a.5.5 0 0 0 .5-.5v-1.5A2.5 2.5 0 0 0 10 9z" clip-rule="evenodd" stroke-width="0.3" stroke="currentColor"/></svg>
												<span>Edit Profile</span>
											</button>
										</form>
									</div>
								</NavItem>
								<NavItem>
									<div class="flex flex-row">
										<form action="/api/logout-google/" method="POST" class="flex flew-row">
											<button type="submit" class="flex items-center w-full">
												<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" class="pr-1">
													<rect width="24" height="24" fill="none" />
													<path fill="currentColor" d="M19.002 3h-14c-1.103 0-2 .897-2 2v4h2V5h14v14h-14v-4h-2v4c0 1.103.897 2 2 2h14c1.103 0 2-.897 2-2V5c0-1.103-.898-2-2-2" />
													<path fill="currentColor" d="m11 16l5-4l-5-4v3.001H3v2h8z" />
												</svg>
												<span>Logout</span>
											</button>
										</form>
									</div>
								</NavItem>
							</NavBar>
						</ul>
					</nav>
				</div>

				<!-- Mobile: avatar/profile icon only -->
				<div class="md:hidden relative">
					<button
						class="flex items-center justify-center rounded-full overflow-hidden w-8 h-8 hover:ring-2 hover:ring-white/50 transition-all"
						onclick={toggleMobileMenu}
						aria-label="Profile menu"
					>
						{#if $displayPicture}
							<img src={$displayPicture} alt="Profile" class="w-8 h-8 rounded-full object-cover" />
						{:else}
							<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 16 16" class="text-white">
								<path fill="currentColor" fill-rule="evenodd" d="M10.5 5a2.5 2.5 0 1 1-5 0a2.5 2.5 0 0 1 5 0m.514 2.63a4 4 0 1 0-6.028 0A4 4 0 0 0 2 11.5V13a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-1.5a4 4 0 0 0-2.986-3.87M8 9H6a2.5 2.5 0 0 0-2.5 2.5V13a.5.5 0 0 0 .5.5h8a.5.5 0 0 0 .5-.5v-1.5A2.5 2.5 0 0 0 10 9z" clip-rule="evenodd"/>
							</svg>
						{/if}
					</button>
				</div>
			{:else}
				<!-- Desktop: full sign-in button -->
				<div class="hidden md:flex items-center justify-center text-white font-bold">
					<form action="/api/login-google/" method="POST">
						<Button type="submit" color="light" pill>
							<img src={google} alt="Google Logo" style="width: 20px; height: 20px; vertical-align: middle; padding-right: 0.5rem;">
							Sign in with Google
						</Button>
					</form>
				</div>

				<!-- Mobile: compact sign-in icon -->
				<div class="md:hidden">
					<form action="/api/login-google/" method="POST">
						<button type="submit" class="flex items-center justify-center text-white p-1 hover:opacity-80 transition-opacity" aria-label="Sign in">
							<img src={google} alt="Sign in with Google" class="w-6 h-6" />
						</button>
					</form>
				</div>
			{/if}

			<DarkMode />
		</div>
	</div>

	<!-- Mobile dropdown menu -->
	{#if mobileMenuOpen}
		<!-- Backdrop -->
		<div
			class="md:hidden fixed inset-0 top-16 bg-black/20 z-90"
			onclick={closeMobileMenu}
			role="presentation"
		></div>

		<!-- Drawer panel -->
		<div class="md:hidden absolute top-16 left-0 right-0 bg-(--color-theme-2) shadow-lg z-100 border-t border-white/10">
			<nav class="flex flex-col py-2">
				<a href="/" onclick={closeMobileMenu} class="flex items-center px-6 py-3 text-white font-medium hover:bg-white/10 transition-colors">
					Home
				</a>

				<!-- About section -->
				<div class="flex flex-col">
					<a href="/about" onclick={closeMobileMenu} class="flex items-center px-6 py-3 text-white font-medium hover:bg-white/10 transition-colors">
						About
					</a>
					<a href="/about#about" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Sakuna PH
					</a>
					<a href="/about#team" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Team
					</a>
				</div>

				<!-- Disasters section -->
				<div class="flex flex-col">
					<a href="/disasters" onclick={closeMobileMenu} class="flex items-center px-6 py-3 text-white font-medium hover:bg-white/10 transition-colors">
						Disasters
					</a>
					<a href="/disasters/map" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Map
					</a>
					<a href="/disasters/table" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Table
					</a>
					<a href="/disasters/metric" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Metric
					</a>
					<a href="/disasters/timeline" onclick={closeMobileMenu} class="flex items-center pl-10 pr-6 py-2 text-white/70 text-sm hover:bg-white/10 transition-colors">
						Timeline
					</a>
				</div>

				<!-- Divider -->
				{#if user}
					<div class="border-t border-white/10 my-2"></div>
					<button
						onclick={() => { toggleModal(); closeMobileMenu(); }}
						class="flex items-center px-6 py-3 text-white font-medium hover:bg-white/10 transition-colors w-full text-left"
					>
						<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 16 16" class="mr-3">
							<path fill="currentColor" fill-rule="evenodd" d="M10.5 5a2.5 2.5 0 1 1-5 0a2.5 2.5 0 0 1 5 0m.514 2.63a4 4 0 1 0-6.028 0A4 4 0 0 0 2 11.5V13a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-1.5a4 4 0 0 0-2.986-3.87M8 9H6a2.5 2.5 0 0 0-2.5 2.5V13a.5.5 0 0 0 .5.5h8a.5.5 0 0 0 .5-.5v-1.5A2.5 2.5 0 0 0 10 9z" clip-rule="evenodd"/>
						</svg>
						Edit Profile
					</button>
					<form action="/api/logout-google/" method="POST">
						<button type="submit" class="flex items-center px-6 py-3 text-white font-medium hover:bg-white/10 transition-colors w-full text-left">
							<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" class="mr-3">
								<rect width="24" height="24" fill="none" />
								<path fill="currentColor" d="M19.002 3h-14c-1.103 0-2 .897-2 2v4h2V5h14v14h-14v-4h-2v4c0 1.103.897 2 2 2h14c1.103 0 2-.897 2-2V5c0-1.103-.898-2-2-2" />
								<path fill="currentColor" d="m11 16l5-4l-5-4v3.001H3v2h8z" />
							</svg>
							Logout
						</button>
					</form>
				{/if}
			</nav>
		</div>
	{/if}

	<FormModal bind:open={showModal} name={$displayName} source={"update"} session={$session}/>
</header>