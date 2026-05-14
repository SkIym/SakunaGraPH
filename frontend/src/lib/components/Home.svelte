
<script>
	import logo from '$lib/images/philippines_logo.svg';
	// import ph_map from '$lib/images/philippines_map.webp';

	import { Carousel, Controls } from "flowbite-svelte";

	const modules = import.meta.glob(
		'$lib/images/carousel/*.jpg',
		{ eager: true, import: 'default' }
	);

	const images = Object.entries(modules).map(([path, src]) => ({
		src,
		alt: "Disaster",
		title: path.split('/').pop().replace('.jpg', '')
	}));
</script>

<svelte:head>
	<link rel="icon" type="image/svg+xml" href={logo}>
	<title>Sakuna PH</title>
	<meta name="description" content="Home Page" />
</svelte:head>

<div class="flex-col px-4 py-8 max-w-300 mx-auto items-center justify-center">
	<div class = "section-title text-center">
		<!-- <h1 class="mt-8 font-bold text-[5rem] text-gray-900">MABUHAY!</h1> -->
		<h2 class="my-10 font-bold text-4xl md:text-6xl lg:text-7xl text-gray-900 dark:text-gray-300">Welcome to Sakuna.PH!</h2>	
		<div class="max-w-240 mx-auto">
			<p class = "text-xl md:text-2xl lg:text-3xl text-gray-900 dark:text-gray-300 font-semibold">A comprehensive, accurate and easy to use database of 100,000+ disaster records from the Philippines since the 1900s.</p>
			<div class="p-8 max-w-200 mx-auto">
				<p class="text-base md:text-lg text-gray-700 dark:text-gray-100">
				Sakuna.PH is a webapp to visualize disaster records from the Philippines in different and easy to use interactive representations.
				The main objective of Sakuna.PH is to create a digital library of disaster records in the Philippines that is accessible to everyone.
				</p>
			
			</div>

			<div class="w-full flex mx-auto justify-center my-8">
				<div class="w-full max-w-4xl">
					<Carousel {images} duration={6000}>
						<Controls />
					</Carousel>
				</div>
			</div>

			<div class="p-8 max-w-200 mx-auto">
				<p class="text-base md:text-lg text-gray-700 dark:text-gray-100">
					Ready to dive in? Read our About for the basic instructions and FAQs.
				</p>
				<br>
				<p class="text-base md:text-lg text-gray-700 dark:text-gray-100">
					Already have a place in mind? Head to Disasters to view the data in many ways.
				</p>
				<br>
				<p class="text-base md:text-lg text-gray-700 dark:text-gray-100">
					Interested in downloading and contributing data? Sign up and Log in.
				</p>
			</div>
		</div>
	</div>
		
	<!-- <div class = "">
		<img alt="Philippine Map" src={ph_map} />
	</div> -->
</div>	


