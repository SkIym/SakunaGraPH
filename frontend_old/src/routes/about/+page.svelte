<script>
	import logo from '$lib/images/philippines_logo.svg';
	import NameCard from '$lib/components/NameCard.svelte'
	import { AccordionItem, Accordion } from "flowbite-svelte";
	import { ChevronDownOutline, ChevronUpOutline } from "flowbite-svelte-icons";

	// import images for team page
	import jake from '$lib/images/team/jake_cadag.jpg';
	import ony from '$lib/images/team/ony_martinez.jpg';
	import abram from '$lib/images/team/abram_marcelo.jpg';
	import judelle from '$lib/images/team/judelle_gaza.jpg';
	import lian from '$lib/images/team/lian_paulo.jpg';
	import lucas from '$lib/images/team/lucas_bumanglag.jpg';
	import raffy from '$lib/images/team/raffy_eclarin.jpg';
</script>

<svelte:head>
	<link rel="icon" type="image/svg+xml" href={logo}>
	<title>Sakuna PH</title>
	<meta name="description" content="About Page" />
</svelte:head>

<div class="max-w-300">
	<section id="about" class="px-4 pt-20">
		<h1 class="font-bold border-b-2 border-gray-400 pb-2 text-[3rem] text-gray-900 dark:text-gray-300">LEARN MORE</h1>
		<div class="mx-auto px-8 py-8 leading-relaxed">	

			<!-- About section -->
			<div class="mt-2">
				<h2 class=" mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">
					About the app
				</h2>

				<p class="mb-4 dark:text-gray-300">
					Sakuna.PH is a collaborative initiative between the University of the Philippines’ Department of Geography (DGE) and
					the Department of Computer Science. Developed and maintained by students as part of their CS 191/192 capstone sequence,
					the platform serves as a centralized hub for Philippine disaster information. 
				</p>
				<p class="mb-4 dark:text-gray-300">
					By allowing users to filter and aggregate data from diverse sources and parameters, the project transforms complex
					datasets into accessible, downloadable formats. Ultimately, Sakuna.PH aims to foster greater public awareness and
					resilience by providing a clear, historical perspective on the disasters that shape our local landscape.
				</p>  
						
			</div>
			
			<!-- Mission -->
			<div class="mt-10 bg-gray-50 p-6 rounded-lg border-l-4 border-(--color-theme-3) dark:bg-gray-800">
				<h2 class="mb-4 text-[1.5rem] font-bold text-gray-900 dark:text-gray-300">Our Mission</h2>
				<p class="italic text-gray-700 dark:text-gray-300">
					"To empower every Filipino with the spatial intelligence needed to build more resilient communities."
				</p>
				<p class="mt-4 dark:text-gray-300">
					By bridging the gap between complex academic datasets and public accessibility, Sakuna.PH serves as a digital library for researchers, local government units, and concerned citizens alike.
				</p>
			</div>
			
			<!-- Data Source section TODO: UPDATE --> 
			<div class="mt-10">
				<h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">
					Data Sources & Integration
				</h2>
				<p class="mb-4 dark:text-gray-300">
					Our platform aggregates and harmonizes data from various national agencies and research institutions to provide a unified view of the Philippine disaster landscape:
				</p>
				<ul class="list-disc ml-6 mb-6 space-y-2 dark:text-gray-300">
					<li><strong>PAGASA:</strong> For hydro-meteorological data and tropical cyclone history.</li>
					<li><strong>PHIVOLCS:</strong> For seismic activity, volcanic eruptions, and tsunami records.</li>
					<li><strong>OCD / NDRRMC:</strong> For situational reports and impact assessments.</li>
					<li><strong>OpenStreetMap & DGE:</strong> For high-resolution geospatial basemaps and exposure data.</li>
				</ul>
			</div>

			<!-- FAQ section -->
			<div class="mt-10">
				<h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">
					Frequently asked questions
				</h2>

				<Accordion activeClass="dark:bg-gray-900 text-black" inactiveClass="bg-gray-100 text-black dark:bg-gray-800">
					<AccordionItem>
						{#snippet header()}<p class="text-black dark:text-gray-300">How do you use the website?</p>{/snippet}
						{#snippet arrowdown()}<ChevronDownOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						{#snippet arrowup()}<ChevronUpOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						<p class="text-black dark:text-gray-300">
							Learn about Sakuna, its team, and general information about Philippine disasters. To view 
							specific records, you must register and login into an account before accessing the disaster page.
						</p>
					</AccordionItem>
					<AccordionItem>
						{#snippet header()}<p class="text-black dark:text-gray-300">Why do I need to register for an account?</p>{/snippet}
						{#snippet arrowdown()}<ChevronDownOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						{#snippet arrowup()}<ChevronUpOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						<p class="mb-2 text-black dark:text-gray-300">
							As the service's database is composed of sensitive government records, we must verify valid user 
							emails and identity before allowing access to the website. It will also help us improve the webapp
							by understanding our user audience.
						</p>
					</AccordionItem>
					<AccordionItem>
						{#snippet header()}<p class="text-black dark:text-gray-300">Is my personal information safe?</p>{/snippet}
						{#snippet arrowdown()}<ChevronDownOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						{#snippet arrowup()}<ChevronUpOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						<p class="mb-2 text-black dark:text-gray-300">
							Yes. Sakuna.PH complies with the <strong>Data Privacy Act of 2012</strong>. We only collect necessary information for account verification and internal analytics. Your personal data is never shared with third parties without explicit consent.
						</p>
					</AccordionItem>
					<AccordionItem>
						{#snippet header()}<p class="text-black dark:text-gray-300">Can I export the data for my own research?</p>{/snippet}
						{#snippet arrowdown()}<ChevronDownOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						{#snippet arrowup()}<ChevronUpOutline class="shrink-0 h-6 w-6 dark:text-gray-300" />{/snippet}
						<p class="mb-2 text-black dark:text-gray-300">
							Registered users can export filtered datasets in CSV or JSON formats. We encourage students and researchers to use this data, provided that proper attribution is given to Sakuna.PH and the original data source.
						</p>
					</AccordionItem>
				</Accordion>
			</div>
		</div>
	</section>

	<section id="team" class="px-4 py-4 pt-20">

		<h1 class="mb-4 font-bold  border-b-2 border-gray-400 pb-2 text-[3rem] text-gray-900 dark:text-gray-300">MEET THE TEAM</h1>
		<div class="flex flex-col mx-auto px-8 py-8 leading-relaxed items-center">
			<div class ="justify-center max-w-240">
				<h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">Contributors</h2>
				<div class="flex flex-col md:flex-row justify-center gap-8 mb-8">
					<NameCard picture={jake} name="Jake Rom Cadag" department="Department of Geography" college="College of Social Sciences and Philosophy"
					university="University of the Philippines Diliman" email="jdcadag@up.edu.ph"></NameCard>

					<NameCard picture={ony} name="Ma. Simeona Martinez" department="Department of Geography" college="College of Social Sciences and Philosophy"
					university="University of the Philippines Diliman" email="mmmartinez@up.edu.ph"></NameCard>
				</div>

				<h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">Database Managers</h2>
				<div class="flex flex-col md:flex-row justify-center gap-8 mb-8">
					<NameCard picture={judelle} name="Judelle Gaza" department="Department of Computer Science" college="College of Engineering"
					university="University of the Philippines Diliman" email="jegaza1@up.edu.ph"></NameCard>

					<NameCard picture={abram} name="Abram Marcelo" department="Department of Computer Science" college="College of Engineering"
					university="University of the Philippines Diliman" email="acmarcelo2@up.edu.ph"></NameCard>				
				</div>

				<h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.8rem] text-gray-900 dark:text-gray-300">Developers</h2>
				<div class="flex flex-col md:flex-row justify-center gap-8 mb-8">
					<NameCard picture={lucas} name="Lucas Bumanglag" department="Department of Computer Science" college="College of Engineering"
					university="University of the Philippines Diliman" email="mobumanglag2@up.edu.ph"></NameCard>

					<NameCard picture={raffy} name="Rafael Eclarin" department="Department of Computer Science" college="College of Engineering"
					university="University of the Philippines Diliman" email="raeclarin@up.edu.ph"></NameCard>	

					<NameCard picture={lian} name="Lian Paulo" department="Department of Computer Science" college="College of Engineering"
					university="University of the Philippines Diliman" email="lmpaulo@up.edu.ph"></NameCard>
				</div>
			</div>
		</div>
	</section>
</div>