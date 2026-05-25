<script lang="ts">
  import { Button, Modal, P } from "flowbite-svelte";
  import { browser } from "$app/environment";
  let defaultModal = $state(false);
  $effect(() => {
    if (browser) {
      defaultModal = true;
    }
  });
</script>

{#if browser}
  <Modal title="Website Status" size="sm" form bind:open={defaultModal} onaction={({ action }) => alert(`Handle "${action}"`)}>
    <P class="text-sm sm:text-base">Sakuna PH is currently under construction. Take note of the following warnings:</P>
    <ul class="list-disc list-inside mt-2 space-y-1 text-sm sm:text-base text-gray-700 dark:text-gray-300">
      <li>Disaster records are currently fictional and may not be accurate.</li>
      <li>Some features may be missing or incomplete.</li>
      <li>Do not share any personal information on this website.</li>
    </ul>
    {#snippet footer()}
      <Button onclick={() => (defaultModal = false)} color="blue" class="w-full sm:w-auto">I accept</Button>
    {/snippet}
  </Modal>
{/if}