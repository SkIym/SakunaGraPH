<svelte:head>
	<title>Sakuna PH</title>
	<meta name="description" content="Login" />
</svelte:head>

<script lang="ts">
  let emailOrUserName = "";
  let password = "";
  let checker = "";

  $: isValid = emailOrUserName.trim() !== "" && password.trim() !== "";

  function checkPasswords() {
    if (isValid) {
      checker = `Successfully login: ${emailOrUserName}`;
    } else {
      checker = "Input is empty.";
    }
  }
</script>


<section class="pt-10">
	<div class="mx-auto mt-10 max-w-150 px-12 py-12 leading-relaxed border rounded-2xl bg-(--color-bg-2) border-(--color-bg-0)">
		<h3 class="text-center font-bold mb-4 border-b-2 border-gray-300 pb-2 text-xl md:text-2xl lg:text-3xl text-gray-900">Login to your account</h3>

		<form on:submit|preventDefault={checkPasswords}>
            
            <!-- <p class="mt-3 mb-1 font-semibold">Username or Email Address</p> -->
            <input
                bind:value={emailOrUserName} 
                class="w-full my-2 rounded-xl border-gray-300 focus:border-(--color-theme-2) focus:ring-(--color-theme-2)"
                placeholder="Username or Email"
                type="text"
                required
            >

            <!-- <p class="mt-3 mb-1 font-semibold">Password</p> -->
            <input 
                bind:value={password}
                class="w-full my-2 rounded-xl border-gray-300 focus:border-(--color-theme-2) focus:ring-(--color-theme-2)"
                placeholder="Password"
                type="password"
                required
            >

            <p class="mt-4 mb-5 text-l font-medium text-amber-600 bg-amber-50 border border-amber-200 p-3 rounded-md">
                Warning: Security implementation in progress. Do not use real passwords.
            </p>
    
            <button 
                type="submit" 
                disabled={!isValid}
                class="
                    group relative w-full flex justify-center py-2.5 px-4 
                    border border-transparent 
                    text-sm font-semibold text-white tracking-wide uppercase
                    rounded-xl shadow-sm 
                    transition-all duration-200 ease-in-out
                    focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-white focus:ring-gray-900
                    bg-(--color-theme-2) hover:opacity-90 hover:shadow-md
                    disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none
                "
            >
                Login
            </button>

            <div class="mt-1 text-sm">Not registered? <a href="/../signup" class="hover:underline text-(--color-theme-2)">Create account</a></div>

            {#if checker}
                <p class="mt-4 text-center text-sm font-medium text-gray-700 animate-pulse">{checker}</p>
            {/if}

        </form>
	</div>
</section>
