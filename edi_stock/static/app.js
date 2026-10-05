"use strict";

(() => {
  const forms = [...document.querySelectorAll("form[data-submit-guard]")];
  const buttons = forms.flatMap((form) => [...form.querySelectorAll('button[type="submit"]')]);
  const initialState = new Map(buttons.map((button) => [button, {
    disabled: button.disabled,
    html: button.innerHTML,
  }]));
  let pending = false;

  const restore = () => {
    pending = false;
    forms.forEach((form) => form.removeAttribute("aria-busy"));
    buttons.forEach((button) => {
      const state = initialState.get(button);
      button.disabled = state.disabled;
      button.innerHTML = state.html;
    });
  };

  forms.forEach((form) => {
    form.addEventListener("submit", (event) => {
      if (pending) {
        event.preventDefault();
        return;
      }
      pending = true;
      form.setAttribute("aria-busy", "true");
      const submitter = event.submitter || form.querySelector('button[type="submit"]');
      buttons.forEach((button) => { button.disabled = true; });
      if (submitter && submitter.dataset.busyLabel) {
        submitter.textContent = submitter.dataset.busyLabel;
      }
    });
  });

  // Back/forward navigation may restore the page with its previous disabled buttons.
  window.addEventListener("pageshow", restore);
})();
