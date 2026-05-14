<script lang="ts">
    import { Button, Modal, Label, Input, Select, MultiSelect } from "flowbite-svelte";
    import { supabase } from "$lib/supabase";
    import { invalidateAll } from '$app/navigation'; 
    import { displayName } from "$lib/authStore";

    let { name, source, open = $bindable(false), session } = $props();
    
    let selected_occupation = $state("");
    let selected_usage = $state([]);
    let loading = $state(false); 

    let profile = $state({ 
        displayName: '',
        birthdate: '',
        address: '',
        affiliation: '',
    });

    let occupation: { value: string; name: string }[] = [
        { value: "Student", name: "Student" },
        { value: "Researcher or Professor", name: "Researcher or Professor" },
        { value: "Government Official", name: "Government Official" },
        { value: "Non-Governmental Organization (NGO)", name: "Non-Governmental Organization (NGO)" },
        { value: "Private Sector", name: "Private Sector" },
        { value: "Unemployed", name: "Unemployed" }
    ];


    let usage: { value: string; name: string }[] = [
        { value: "Learning Studies", name: "Learning Studies" },
        { value: "Academic Research", name: "Academic Research" },
        { value: "Publishing Content", name: "Publishing Content" },
        { value: "Government Policy-making", name: "Government Policy-making" },
        { value: "Disaster Preparation", name: "Disaster Preparation" },
        { value: "Personal Use", name: "Personal Use" },
        { value: "Business and Industry", name: "Business and Industry" },
        { value: "Humanitarian Activities", name: "Humanitarian Activities" },
        { value: "Community Support", name: "Community Support" }
    ];

    async function handleSubmit(event: Event) {
        event.preventDefault(); 
        if (loading) return;

        try {
            loading = true;
            const user = session?.user;
            if (!user) throw new Error("No active session found.");

            const updates = {
                id: user.id, 
                display_name: profile.displayName, 
                birthdate: profile.birthdate,
                city_location: profile.address,
                affiliation: profile.affiliation,
                occupation: selected_occupation,
                intended_usage: selected_usage,
                updated_at: new Date(),
            };

            const { error } = await supabase
                .from('profiles')
                .upsert(updates, { onConflict: 'id' });

            if (error) throw error;

            
            await invalidateAll();
            open = false;

            const url = new URL(window.location.href);
            url.searchParams.delete('setup');
            window.history.replaceState({}, '', url);

            displayName.set(profile.displayName);

            alert('Profile setup complete!');
        } catch (error) {
            alert(error.message);
        } finally {
            loading = false;
        }
    }    
</script>

<!-- <Button onclick={() => (formModal = true)}>Form modal</Button> -->

<Modal bind:open={open} size="md" outsideclose={false} dismissable={source === "setup" ? false : true}>
    <form onsubmit={handleSubmit} class="flex flex-col space-y-4 max-w-150 mx-auto px-2">
        {#if source === "setup"}
            <h3 class="text-center font-bold mx-4 mt-2 mb-2 text-[1.8rem] sm:text-[2.5rem] dark:text-gray-100! text-black">You're almost there, {name}!</h3>
            <h3 class="text-center font-bold mx-2 mb-4 border-b-2 border-gray-300 pb-2 text-[1.1rem] sm:text-[1.4rem] dark:text-gray-100! text-black">Complete the form below to finish your account setup.</h3>
        {:else if source === "update"}
            <h3 class="text-center font-bold mx-4 mt-2 mb-2 text-[1.8rem] sm:text-[2.5rem] dark:text-gray-100! text-black">Update your profile, {name}!</h3>
            <h3 class="text-center font-bold mx-2 mb-4 border-b-2 border-gray-300 pb-2 text-[1.1rem] sm:text-[1.4rem] dark:text-gray-100! text-black">Make changes to your profile information below.</h3>
        {/if}

        <!-- Stacks vertically on mobile, side by side on sm+ -->
        <div class="flex flex-col sm:flex-row gap-4">
            <div class="w-full">
                <Label for="name" class="mb-2">Display Name</Label>
                <Input id="name" placeholder="Enter your display name" bind:value={profile.displayName}/>
            </div>
            <div class="w-full">
                <Label for="birthdate" class="mb-2">Birthdate</Label>
                <Input type="date" placeholder="Select your birthdate" bind:value={profile.birthdate} />
            </div>
        </div>

        <div>
            <Label>
                Occupation
                <Select class="mt-2 text-gray-100!" items={occupation} bind:value={selected_occupation} placeholder="Select your occupation" />
            </Label>
        </div>

        <div>
            <Label for="address" class="mb-2 block">Address</Label>
            <Input id="address" placeholder="Enter your address (e.g. Quezon City, Metro Manila)" bind:value={profile.address} />
        </div>

        <div>
            <Label for="affiliation" class="mb-2 block">Affiliation</Label>
            <Input id="affiliation" placeholder="Enter your university or organization" bind:value={profile.affiliation}/>
        </div>

        <div>
            <Label for="usage">Intended Usage</Label>
            <MultiSelect id="usage" class="mt-2" items={usage} bind:value={selected_usage} placeholder="Select intended usage" />
        </div>

        <p class="text-sm font-medium text-amber-800 bg-amber-100 border border-amber-200 p-3 rounded-md">
            Warning: Make sure information is correct before submitting.
        </p>

        <Button type="submit" class="mb-4" value="submit" color="blue">Submit</Button>
    </form>
</Modal>